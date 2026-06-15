from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)
from pyspark.sql.functions import col, from_json

schema = StructType([
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True)
])

spark = (
    SparkSession.builder
    .appName("KafkaStreaming")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "users_topic")
    .option("startingOffsets", "earliest")
    .load()
)

parsed_df = df.selectExpr(
    "CAST(value AS STRING)"
)

json_df = parsed_df.select(
    from_json(
        col("value"),
        schema
    ).alias("data")
)

final_df = json_df.select(
    "data.*"
)

query = (
    final_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "output/users")
    .option("checkpointLocation", "output/checkpoints")
    .start()
)

query.awaitTermination()