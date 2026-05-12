from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, when
from pyspark.sql.types import StructType, StringType

spark = SparkSession.builder \
    .appName("EcommerceClickstreamStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("user_id", StringType()) \
    .add("product_id", StringType()) \
    .add("category", StringType()) \
    .add("event_type", StringType()) \
    .add("timestamp", StringType())

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "ecommerce_events") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Convert JSON
events = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Convert timestamp
events = events.withColumn(
    "event_time",
    col("timestamp").cast("timestamp")
)

# Add watermark
events = events.withWatermark("event_time", "10 minutes")

# Window aggregation
summary = events.groupBy(
    window(col("event_time"), "10 minutes", "5 minutes"),
    col("product_id"),
    col("category")
).agg(
    count(
        when(col("event_type") == "view", True)
    ).alias("views"),

    count(
        when(col("event_type") == "purchase", True)
    ).alias("purchases")
)

# Flash sale alerts
alerts = summary.filter(
    (col("views") > 100) &
    (col("purchases") < 5)
)

# Save raw events
raw_query = events.writeStream \
    .format("parquet") \
    .option("path", "/opt/data/raw") \
    .option("checkpointLocation", "/opt/data/checkpoints/raw") \
    .outputMode("append") \
    .start()

# Save alerts
alert_query = alerts.writeStream \
    .format("parquet") \
    .option("path", "/opt/data/alerts") \
    .option("checkpointLocation", "/opt/data/checkpoints/alerts") \
    .outputMode("append") \
    .start()

# Print alerts to console
console_query = alerts.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .start()

raw_query.awaitTermination()
alert_query.awaitTermination()
console_query.awaitTermination()