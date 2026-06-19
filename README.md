# Real-Time Kafka Streaming Data Lake Pipeline

## Overview

This project implements a real-time streaming data pipeline using Apache Kafka, Apache Spark Structured Streaming, Amazon S3, and Apache Airflow.

The pipeline continuously generates user events, streams them through Kafka, processes them using Spark Structured Streaming, and stores them as Parquet files in an AWS S3 Data Lake. Apache Airflow orchestrates the end-to-end workflow and validates successful data ingestion.

This project demonstrates modern Data Engineering concepts including event streaming, stream processing, cloud storage, workflow orchestration, checkpointing, and fault-tolerant data ingestion.

---

## Architecture

```text
                Apache Airflow
             Workflow Orchestration
                       |
                       v

+------------+    +------------+    +----------------------+
|   Python   | -> |   Kafka    | -> | Spark Structured     |
|  Producer  |    |   Topic    |    | Streaming            |
+------------+    +------------+    +----------+-----------+
                                             |
                                             v

                              +----------------------------+
                              | AWS S3 Data Lake           |
                              | Parquet Storage            |
                              +-------------+--------------+
                                            |
                                            v

                              +----------------------------+
                              | Validation Layer           |
                              | S3 Output Verification     |
                              +----------------------------+
```

---

## Features

* Real-time event generation using Python
* Apache Kafka-based message streaming
* Spark Structured Streaming processing
* JSON schema validation
* Automatic Parquet file generation
* AWS S3 Data Lake storage
* Streaming checkpoint management
* Apache Airflow workflow orchestration
* Data validation after ingestion
* Fault-tolerant stream processing

---

## Tech Stack

### Streaming

* Apache Kafka
* Kafka Producer API

### Processing

* Apache Spark Structured Streaming
* PySpark

### Cloud

* Amazon S3

### Orchestration

* Apache Airflow

### Containerization

* Docker
* Docker Compose

### Language

* Python

---

## Project Structure

```text
kafka-spark-airflow-pipeline/

├── producer/
│   └── producer.py
│
├── spark/
│   └── spark_stream.py
│
├── airflow/
│   ├── dags/
│   │   └── kafka_pipeline_dag.py
│   └── docker-compose.yml
│
├── utils/
│   └── check_s3_output.py
│
├── screenshots/
│
├── requirements.txt
│
└── README.md
```

---

## Pipeline Workflow

### Step 1: Event Generation

A Python producer generates user events and publishes them to a Kafka topic.

Example Event:

```json
{
  "first_name": "John",
  "last_name": "Doe",
  "email": "john@example.com",
  "country": "USA"
}
```

---

### Step 2: Kafka Streaming

Events are published to:

```text
users_topic
```

Kafka acts as the streaming backbone of the pipeline.

---

### Step 3: Spark Structured Streaming

Spark continuously consumes events from Kafka.

Operations performed:

* Read stream from Kafka
* Parse JSON payload
* Apply schema validation
* Transform records
* Write output to S3

---

### Step 4: S3 Data Lake Storage

Processed records are stored as:

```text
s3://kafka-spark-airflow-lake/streaming/users/
```

Format:

```text
Parquet
```

Benefits:

* Columnar storage
* Compression
* Faster analytics
* Reduced storage cost

---

### Step 5: Checkpointing

Spark maintains checkpoints in:

```text
s3://kafka-spark-airflow-lake/checkpoints/
```

This enables:

* Fault tolerance
* Recovery from failures
* Exactly-once processing semantics

---

### Step 6: Airflow Orchestration

Apache Airflow orchestrates the workflow:

1. Execute Producer
2. Run Spark Streaming Job
3. Validate S3 Output

DAG Flow:

```text
run_producer
      ↓
run_spark_stream
      ↓
validate_s3_output
```

---

## Running the Project

### Start Kafka

```bash
docker compose up -d
```

### Start Producer

```bash
python producer/producer.py
```

### Start Spark Streaming

```bash
python spark/spark_stream.py
```

### Trigger Airflow DAG

Open:

```text
http://localhost:8081
```

Trigger:

```text
kafka_pipeline
```

---

## Sample Output

### Kafka Event

```json
{
  "first_name": "Alice",
  "last_name": "Brown",
  "email": "alice@example.com",
  "country": "Canada"
}
```

### S3 Output

```text
streaming/users/
├── part-00000.parquet
├── part-00001.parquet
└── ...
```

---

## Key Data Engineering Concepts Demonstrated

* Event-Driven Architecture
* Real-Time Streaming
* Structured Streaming
* Data Lake Design
* Parquet Storage Optimization
* Workflow Orchestration
* Fault Tolerance
* Checkpointing
* Cloud Storage Integration
* Stream Processing

---

## Future Enhancements

* AWS Athena Integration
* Glue Catalog Registration
* Data Quality Framework
* Partitioned Parquet Tables
* Real-Time Dashboarding
* Snowflake Integration
* dbt Transformations
* CI/CD Deployment Pipeline

---

## Author

Nithin Rajan

Data Engineer | AWS | Spark | Kafka | Airflow | Python
