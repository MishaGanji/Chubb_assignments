from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "misha",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="retail_sales_end_to_end_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    #schedule="@daily",   
    catchup=False,
    tags=["retail", "databricks", "capstone"],
) as dag:

    DatabricksRunNowOperator(
        task_id="trigger_databricks_retail_job",
        databricks_conn_id="databricks_default",
        job_id=483109031196298
    )
