import boto3

s3 = boto3.client("s3")

response = s3.list_objects_v2(
    Bucket="kafka-spark-airflow-lake",
    Prefix="streaming/users/"
)

count = response.get("KeyCount", 0)

if count > 0:
    print("Parquet files found")
else:
    raise Exception("No parquet files found")