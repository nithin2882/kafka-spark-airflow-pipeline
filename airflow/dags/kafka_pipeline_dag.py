from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="kafka_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["kafka", "spark", "s3"]
) as dag:

    producer_task = BashOperator(
        task_id="run_producer",
        bash_command="""
        cd /opt/airflow/project &&
        python producer/producer.py
        """
    )

    spark_task = BashOperator(
        task_id="run_spark_stream",
        bash_command="""
        timeout 60 python /opt/airflow/project/spark/spark_stream.py
        """
    )

    validation_task = BashOperator(
        task_id="validate_s3_output",
        bash_command="""
        python /opt/airflow/project/utils/check_s3_output.py
        """
    )

    producer_task >> spark_task >> validation_task