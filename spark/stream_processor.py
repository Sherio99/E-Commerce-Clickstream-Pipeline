from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, expr
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

spark = SparkSession.builder \
    .appName("Clickstream-Stream-Processor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", StringType(), True),
])

kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "clickstream-events") \
    .load()

parsed = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", col("timestamp").cast(TimestampType()))

# ✅ Add watermark BEFORE aggregation
parsed = parsed.withWatermark("event_time", "10 minutes")

aggregated = parsed.groupBy(
    window(col("event_time"), "10 minutes", "5 minutes"),
    col("product_id")
).agg(
    expr("sum(case when event_type='view' then 1 else 0 end) as views"),
    expr("sum(case when event_type='purchase' then 1 else 0 end) as purchases")
)

alerts = aggregated.filter(
    (col("views") > 100) & (col("purchases") < 5)
).select(
    col("product_id"),
    col("views"),
    col("purchases"),
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end")
)

# ✅ Raw clickstream events for Airflow user segmentation
parsed.writeStream \
    .format("parquet") \
    .option("path", "/opt/airflow/data/raw") \
    .option("checkpointLocation", "/opt/airflow/data/checkpoints/raw") \
    .outputMode("append") \
    .start()

aggregated.writeStream \
    .format("parquet") \
    .option("path", "/opt/airflow/data/processed") \
    .option("checkpointLocation", "/opt/airflow/data/checkpoints/agg") \
    .outputMode("append") \
    .start()

alerts.writeStream \
    .format("json") \
    .option("path", "/opt/airflow/data/alerts") \
    .option("checkpointLocation", "/opt/airflow/data/checkpoints/alerts") \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()