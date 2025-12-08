import pendulum
import os
import time
from airflow.sdk import dag, task, get_current_context
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.smtp.operators.smtp import EmailOperator


# DAG DEFINITION
@dag(
    dag_id="shopverse_daily_pipeline",
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2025, 1, 1),
    catchup=True,
    tags=["shopverse"]
)
def shopverse_pipeline_daily():

    base_path = Variable.get("shopverse_data_base_path")

    customers_path = f"{base_path}/landing/customers/customers_{{{{ ds_nodash }}}}.csv"
    products_path  = f"{base_path}/landing/products/products_{{{{ ds_nodash }}}}.csv"
    orders_path    = f"{base_path}/landing/orders/orders_{{{{ ds_nodash }}}}.json"


    # CUSTOM FILE SENSOR (reliable → works everywhere)
    @task()
    def wait_for_file(path: str):
        ctx = get_current_context()
        print(f"🔍 Waiting for file: {path}")

        timeout = 60 * 20
        start = time.time()

        while time.time() - start < timeout:
            if os.path.exists(path):
                print(f"✅ File found: {path}")
                return True
            time.sleep(10)

        raise FileNotFoundError(f"❌ File not found: {path}")

    wait_customers = wait_for_file(customers_path)
    wait_products  = wait_for_file(products_path)
    wait_orders    = wait_for_file(orders_path)


    # STAGING GROUP
    with TaskGroup("staging") as staging_group:

        @task()
        def stage_customers():
            ctx = get_current_context()
            print(f"Staging customers for {ctx['ds_nodash']}")

        @task()
        def stage_products():
            ctx = get_current_context()
            print(f"Staging products for {ctx['ds_nodash']}")

        @task()
        def stage_orders():
            ctx = get_current_context()
            print(f"Staging orders for {ctx['ds_nodash']}")

        stage_customers() >> stage_products() >> stage_orders()


    # WAREHOUSE GROUP
    with TaskGroup("warehouse") as warehouse_group:

        @task()
        def build_dim_customers():
            print("Building dim_customers")

        @task()
        def build_dim_products():
            print("Building dim_products")

        @task()
        def build_fact_orders():
            print("Building fact_orders")

        build_dim_customers() >> build_dim_products() >> build_fact_orders()


    # DATA QUALITY CHECKS
    with TaskGroup("data_quality") as dq_group:

        @task()
        def dq_check_customer_dim():
            hook = PostgresHook("postgres_dwh")
            count = hook.get_first("SELECT COUNT(*) FROM dim_customers;")[0]
            if count < 1:
                raise ValueError("DQ FAILED: dim_customers is empty")
            return count


        @task()
        def dq_check_fact_orders_not_empty():
            ctx = get_current_context()
            ds = ctx["logical_date"].strftime("%Y-%m-%d")

            hook = PostgresHook("postgres_dwh")
            count = hook.get_first(
                f"""
                SELECT COUNT(*)
                FROM fact_orders
                WHERE order_timestamp::date = '{ds}';
                """
            )[0]

            if count < 1:
                raise ValueError("DQ FAILED: No orders loaded for run date")

            return count


        @task()
        def dq_check_fact_orders_nulls():
            hook = PostgresHook("postgres_dwh")
            bad = hook.get_first(
                """
                SELECT COUNT(*)
                FROM fact_orders
                WHERE customer_id IS NULL
                   OR product_id IS NULL;
                """
            )[0]

            if bad > 0:
                raise ValueError("DQ FAILED: Null FK values found")

            return "OK"


        dq_check_customer_dim()
        dq_check_fact_orders_not_empty()
        dq_check_fact_orders_nulls()


    # BRANCHING LOGIC
    @task()
    def check_order_volume():
        ctx = get_current_context()
        ds = ctx["logical_date"].strftime("%Y-%m-%d")

        hook = PostgresHook("postgres_dwh")
        count = hook.get_first(
            f"""
            SELECT COUNT(*)
            FROM fact_orders
            WHERE order_timestamp::date = '{ds}';
            """
        )[0]

        threshold = int(Variable.get("shopverse_min_order_threshold", 10))

        if count < threshold:
            return "warn_low_volume"
        return "normal_flow"


    @task()
    def warn_low_volume():
        ctx = get_current_context()
        print(f"⚠ LOW ORDER VOLUME on {ctx['logical_date']}")


    @task()
    def normal_flow():
        ctx = get_current_context()
        print(f"Normal volume on {ctx['logical_date']}")


    # EMAIL ON FAILURE
    failure_alert = EmailOperator(
        task_id="failure_alert",
        to="your_email@gmail.com",
        subject="ShopVerse Pipeline FAILED",
        html_content="<h3>Pipeline failed. Check Airflow logs.</h3>",
        trigger_rule="one_failed",
    )


    # DAG FLOW
    [wait_customers, wait_products, wait_orders] >> staging_group >> warehouse_group >> dq_group

    branch = check_order_volume()

    branch >> warn_low_volume()
    branch >> normal_flow()

    dq_group >> failure_alert


shopverse_pipeline_daily()
