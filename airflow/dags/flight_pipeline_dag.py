"""
Apache Airflow DAG: Flight & Airport Big Data Pipeline
Owner: Member 1 (Infrastructure & Orchestration)
Flow: check_data -> csv_to_parquet -> spark_aggregates -> load_clickhouse -> train_model
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'member1_infrastructure',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'flight_bigdata_pipeline',
    default_args=default_args,
    description='End-to-End Flight Analytics Pipeline: HDFS -> Spark -> ClickHouse -> ML',
    schedule_interval=None,  # Manual trigger for sprint
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bigdata', 'spark', 'clickhouse', 'flights', 'nti'],
) as dag:

    # Task 1: Check raw data files in HDFS
    check_data = BashOperator(
        task_id='check_data',
        bash_command='hdfs dfs -ls /project/flights/raw/ && echo "HDFS raw flight data verified."',
    )

    # Task 2: Execute CSV to Parquet conversion on Spark cluster
    csv_to_parquet = BashOperator(
        task_id='csv_to_parquet',
        bash_command='/opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/spark_csv_to_parquet.py',
    )

    # Task 3: Compute all 7 analytical aggregations on Spark
    spark_aggregates = BashOperator(
        task_id='spark_aggregates',
        bash_command='/opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/spark_aggregates.py',
    )

    # Task 4: Bulk-load generated aggregations into ClickHouse
    load_clickhouse = BashOperator(
        task_id='load_clickhouse',
        bash_command="""
        for f in /opt/spark-apps/output/*.csv; do
            table_name=$(basename "$f" .csv)
            echo "Loading $table_name into ClickHouse..."
            clickhouse-client --host clickhouse --user default --password clickhouse \
                --query "INSERT INTO flight_analytics.$table_name FORMAT CSVWithNames" < "$f"
        done
        echo "All aggregations loaded into ClickHouse."
        """,
    )

    # Task 5: Train MLlib delay prediction classifier and export predictions
    train_model = BashOperator(
        task_id='train_model',
        bash_command='/opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/spark_delay_ml.py',
    )

    # Define linear execution pipeline
    check_data >> csv_to_parquet >> spark_aggregates >> load_clickhouse >> train_model
