# Kafka Spark Streaming Pipeline

Real-time data pipeline using:

- Kafka
- Spark Structured Streaming
- Docker
- Kafka UI

Pipeline Flow:

Producer → Kafka → Spark Streaming → Console

Features:
- Real-time event ingestion
- JSON parsing with Spark
- Kafka topic monitoring
- Dockerized local environment

Architecture:

Random User API
        ↓
Kafka Producer
        ↓
Kafka Topic
        ↓
Spark Structured Streaming
        ↓
Parquet Files
        ↓
Local Data Lake
        ↓
Airflow DAG
        ↓
Data Quality Check


Source System
    ↓
Streaming Layer
    ↓
Processing Layer
    ↓
Data Lake
    ↓
Orchestration Layer