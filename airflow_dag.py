from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from datetime import datetime

default_args = {
    "owner": "indra",
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}

with DAG(
    dag_id="ecommerce_data_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    # Task 1: Run Spark ETL
    run_spark_job = BashOperator(
        task_id="run_spark_etl",
        bash_command="spark-submit /path/to/spark_etl.py"
    )

    # Task 2: Load to Redshift
    load_to_redshift = RedshiftSQLOperator(
        task_id="load_to_redshift",
        sql="""
            COPY customer_revenue
            FROM 's3://your-bucket/processed-data/customer_revenue/'
            IAM_ROLE 'arn:aws:iam::your-account-id:role/RedshiftRole'
            FORMAT AS PARQUET;
        """,
        redshift_conn_id="redshift_default"
    )

    run_spark_job >> load_to_redshift
