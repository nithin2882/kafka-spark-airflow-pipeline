import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)
from pyspark.sql.functions import col, from_json
import sys

run_id = sys.argv[1]

load_dotenv()
bucket = os.getenv("S3_BUCKET_NAME")

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS"
)

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
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2",
            "org.apache.hadoop:hadoop-aws:3.4.1"
        ])
    )
    .getOrCreate()
)

hadoop_conf = spark._jsc.hadoopConfiguration()

hadoop_conf.set(
    "fs.s3a.access.key",
    os.getenv("AWS_ACCESS_KEY_ID")
)

hadoop_conf.set(
    "fs.s3a.secret.key",
    os.getenv("AWS_SECRET_ACCESS_KEY")
)

hadoop_conf.set(
    "fs.s3a.endpoint",
    "s3.amazonaws.com"
)

spark.sparkContext.setLogLevel("WARN")

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", "users_topic")
    .option("startingOffsets", "latest")
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
    .option(
    "path",
    f"s3a://kafka-spark-airflow-lake/streaming/users/{run_id}/"
    )
    .option(
    "checkpointLocation",
    f"s3a://kafka-spark-airflow-lake/checkpoints/{run_id}/"
    )
)

query.awaitTermination(60)
query.stop()