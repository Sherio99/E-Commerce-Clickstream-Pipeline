from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("category-conversion-report").getOrCreate()

processed = spark.read.parquet("/opt/airflow/data/processed")
raw = spark.read.parquet("/opt/airflow/data/raw")

# Build product -> category mapping from raw events
# (pick any category seen for that product_id; max() works if category is consistent)
product_category = (raw
    .select("product_id", "category")
    .where(F.col("product_id").isNotNull() & F.col("category").isNotNull())
    .groupBy("product_id")
    .agg(F.max("category").alias("category"))
)

# Join aggregated metrics with category
enriched = processed.join(product_category, on="product_id", how="left")

# Aggregate per category and compute conversion rate
report = (enriched
    .groupBy("category")
    .agg(
        F.sum("views").cast("long").alias("views"),
        F.sum("purchases").cast("long").alias("purchases"),
    )
    .withColumn(
        "conversion_rate",
        F.when(F.col("views") > 0, F.col("purchases") / F.col("views")).otherwise(F.lit(0.0))
    )
    .orderBy(F.desc("conversion_rate"))
)

out_dir = "/opt/airflow/data/reports/_category_conversion_tmp"
(report.coalesce(1)
    .write.mode("overwrite")
    .option("header", True)
    .csv(out_dir)
)

spark.stop()
print("WROTE:", out_dir)