from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    logger.info("Starting Clickstream Stream Processor...")
    
    spark = SparkSession.builder \
        .appName("Clickstream-Stream-Processor") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark Session created successfully")
    
    schema = StructType([
        StructField("user_id", StringType()),
        StructField("product_id", StringType()),
        StructField("category", StringType()),
        StructField("event_type", StringType()),
        StructField("timestamp", StringType())
    ])
    logger.info("Schema defined")
    
    logger.info("Connecting to Kafka broker...")
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "clickstream-events") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    logger.info("Kafka connection established")
    
    logger.info("Parsing JSON events...")
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    parsed_df = parsed_df.withColumn(
        "timestamp",
        col("timestamp").cast(TimestampType())
    )
    logger.info("Timestamp conversion completed")
    
    logger.info("Setting up real-time product aggregation...")
    agg_df = parsed_df \
        .withWatermark("timestamp", "5 minutes") \
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            col("category")
        ) \
        .agg(
            count("*").alias("total_events"),
            count(col("event_type") == "purchase").alias("purchases"),
            count(col("event_type") == "view").alias("views")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("category"),
            col("total_events"),
            col("purchases"),
            col("views")
        )
    
    logger.info("Setting up alert trigger logic...")
    alerts_df = parsed_df \
        .withWatermark("timestamp", "5 minutes") \
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            col("product_id"),
            col("category")
        ) \
        .agg(
            count("*").alias("event_count")
        ) \
        .filter(col("event_count") > 100) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("product_id"),
            col("category"),
            col("event_count")
        )
    
    logger.info("Starting raw events stream to parquet...")
    query1 = parsed_df.writeStream \
        .format("parquet") \
        .option("path", "/tmp/clickstream_raw") \
        .option("checkpointLocation", "/tmp/checkpoint_raw") \
        .partitionBy("category") \
        .start()
    
    logger.info("Starting aggregated metrics stream to parquet...")
    query2 = agg_df.writeStream \
        .format("parquet") \
        .option("path", "/tmp/clickstream_agg") \
        .option("checkpointLocation", "/tmp/checkpoint_agg") \
        .start()
    
    logger.info("Starting alerts stream to CSV...")
    query3 = alerts_df.writeStream \
        .format("csv") \
        .option("path", "/tmp/clickstream_alerts") \
        .option("checkpointLocation", "/tmp/checkpoint_alerts") \
        .option("header", "true") \
        .start()
    
    logger.info("=" * 60)
    logger.info("All streams started successfully!")
    logger.info("=" * 60)
    logger.info("Waiting for incoming data...")
    logger.info("Press Ctrl+C to stop the processor\n")
    
    spark.streams.awaitAnyTermination()
    
except KeyboardInterrupt:
    logger.info("\nProcessor interrupted by user")
except Exception as e:
    logger.error(f"Fatal error in stream processor: {e}", exc_info=True)
finally:
    if 'spark' in locals():
        spark.stop()
        logger.info("Spark session closed")
