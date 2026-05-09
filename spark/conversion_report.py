from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, round, to_date, lit, current_date
from pyspark.sql.types import DoubleType
import logging
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    logger.info("Starting Daily Conversion Rate Analytics...")
    
    # Create Spark Session
    spark = SparkSession.builder \
        .appName("Daily-Conversion-Analytics") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark Session created successfully")
    
    RAW_PATH = "/opt/airflow/data/raw/streaming"
    REPORT_PATH = "/opt/airflow/data/reports/conversion_rate_report"
    
    # Create report directory
    os.makedirs(REPORT_PATH, exist_ok=True)
    logger.info(f"Report output directory: {REPORT_PATH}")
    
    # ============================================================
    # READ RAW CLICKSTREAM DATA
    # ============================================================
    logger.info(f"Reading raw clickstream data from {RAW_PATH}...")
    
    try:
        df = spark.read.parquet(RAW_PATH)
        logger.info(f"Successfully loaded parquet data. Total records: {df.count()}")
    except Exception as e:
        logger.error(f"Error reading parquet files: {e}")
        logger.info("No data available yet. Creating empty report...")
        
        # Create empty report structure
        from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
        empty_schema = StructType([
            StructField("category", StringType(), True),
            StructField("views", LongType(), True),
            StructField("purchases", LongType(), True),
            StructField("cart_adds", LongType(), True),
            StructField("conversion_rate", DoubleType(), True),
            StructField("cart_to_purchase_rate", DoubleType(), True)
        ])
        
        report = spark.createDataFrame([], empty_schema)
        report.write.mode("overwrite").csv(
            REPORT_PATH,
            header=True
        )
        logger.info(f"Empty report saved to {REPORT_PATH}")
        spark.stop()
        exit(0)
    
    # ============================================================
    # DATA PREPROCESSING
    # ============================================================
    logger.info("Preprocessing data...")
    
    # Convert timestamp to datetime
    df = df.withColumn("event_date", to_date(col("timestamp")))
    
    # Select required columns
    df = df.select(
        col("product_id"),
        col("category"),
        col("event_type"),
        col("event_date")
    )
    
    # Drop rows with missing category
    df = df.filter(col("category").isNotNull())
    
    logger.info(f"Records after preprocessing: {df.count()}")
    
    # ============================================================
    # AGGREGATE METRICS BY CATEGORY
    # ============================================================
    logger.info("Aggregating metrics by category...")
    
    # Count views per category
    views_df = df.filter(col("event_type") == "view") \
        .groupBy("category") \
        .agg(count("*").alias("views"))
    
    # Count purchases per category
    purchases_df = df.filter(col("event_type") == "purchase") \
        .groupBy("category") \
        .agg(count("*").alias("purchases"))
    
    # Count cart additions per category
    cart_df = df.filter(col("event_type") == "add_to_cart") \
        .groupBy("category") \
        .agg(count("*").alias("cart_adds"))
    
    logger.info("Joining aggregation results...")
    
    # Join all metrics
    report = views_df.join(purchases_df, on="category", how="left") \
        .join(cart_df, on="category", how="left") \
        .fillna(0)
    
    # ============================================================
    # CALCULATE CONVERSION RATES WITH SAFEGUARDS
    # ============================================================
    logger.info("Calculating conversion rates...")
    
    # Conversion Rate: Purchases / Views (with zero check)
    report = report.withColumn(
        "conversion_rate",
        when(col("views") > 0, 
             round(col("purchases").cast(DoubleType()) / col("views"), 4))
        .otherwise(0.0)
    )
    
    # Cart-to-Purchase Rate: Purchases / Cart Adds (with zero check)
    report = report.withColumn(
        "cart_to_purchase_rate",
        when(col("cart_adds") > 0,
             round(col("purchases").cast(DoubleType()) / col("cart_adds"), 4))
        .otherwise(0.0)
    )
    
    # ============================================================
    # SORT AND PREPARE FOR OUTPUT
    # ============================================================
    logger.info("Sorting report by conversion rate (descending)...")
    
    report = report.select(
        col("category"),
        col("views").cast("long"),
        col("purchases").cast("long"),
        col("cart_adds").cast("long"),
        col("conversion_rate"),
        col("cart_to_purchase_rate")
    ).orderBy(col("conversion_rate").desc())
    
    # ============================================================
    # DISPLAY AND SAVE REPORT
    # ============================================================
    logger.info("\n" + "="*80)
    logger.info("DAILY CONVERSION RATE REPORT")
    logger.info("="*80)
    
    report.show(truncate=False)
    
    logger.info("\nReport Summary:")
    logger.info(f"Total Categories: {report.count()}")
    
    avg_conversion = report.agg({"conversion_rate": "avg"}).collect()[0][0]
    logger.info(f"Average Conversion Rate: {avg_conversion:.4f}")
    
    # Save report to CSV
    logger.info(f"\nSaving report to {REPORT_PATH}...")
    
    report.write.mode("overwrite").csv(
        REPORT_PATH,
        header=True
    )
    
    logger.info(f"Report saved successfully!")
    logger.info("="*80 + "\n")
    
    # ============================================================
    # GENERATE ADDITIONAL INSIGHTS
    # ============================================================
    logger.info("Generating additional insights...")
    
    # Top performing categories
    top_conversion = report.orderBy(col("conversion_rate").desc()).limit(3)
    logger.info("\nTop 3 Converting Categories:")
    top_conversion.show(truncate=False)
    
    # Low performing categories (with views but no purchases)
    low_conversion = report.filter(
        (col("views") > 10) & (col("purchases") == 0)
    ).orderBy(col("views").desc()).limit(3)
    
    if low_conversion.count() > 0:
        logger.info("\nCategories with High Views but No Purchases:")
        low_conversion.show(truncate=False)
    
    logger.info("\nConversion report generation completed successfully!")
    
except Exception as e:
    logger.error(f"Fatal error in conversion report generation: {e}", exc_info=True)
    raise

finally:
    spark.stop()
    logger.info("Spark Session closed")