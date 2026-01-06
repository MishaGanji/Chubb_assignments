from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "misha",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_pipeline_sales_retail",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,   
    catchup=False,
    tags=["capstone", "bronze-silver-gold", "databricks"],
) as dag:

    start_pipeline = EmptyOperator(
        task_id="start_pipeline"
    )

    bronze_ingestion = DatabricksRunNowOperator(
        task_id="bronze_ingestion",
        databricks_conn_id="databricks_default",
        job_id=180911638521858
    )

    silver_transformation = DatabricksRunNowOperator(
        task_id="silver_transformation",
        databricks_conn_id="databricks_default",
        job_id=41046500473195
    )

    gold_aggregation = DatabricksRunNowOperator(
        task_id="gold_aggregation",
        databricks_conn_id="databricks_default",
        job_id=703684715688385
    )

    end_pipeline = EmptyOperator(
        task_id="end_pipeline"
    )

    start_pipeline >> bronze_ingestion >> silver_transformation >> gold_aggregation >> end_pipeline
