from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from extract_postgres import main as extract_postgres_main
from extract_currency_api import main as extract_currency_main
from land_raw_s3 import land_raw
from validate_raw import main as validate_raw_main
from load_to_snowflake_stage import main as load_to_snowflake_stage_main

with DAG(
    dag_id="daily_retail_pipeline",
    start_date=datetime(2026, 7, 1),
    schedule="@daily",
    catchup=False,
    tags=["retail", "lakehouse"],
) as dag:
    start = EmptyOperator(task_id="start")

    extract_postgres = PythonOperator(
        task_id="extract_postgres",
        python_callable=extract_postgres_main,
    )

    extract_currency_api = PythonOperator(
        task_id="extract_currency_api",
        python_callable=extract_currency_main,
    )

    land_raw_s3 = PythonOperator(
        task_id="land_raw_s3",
        python_callable=land_raw,
    )

    validate_raw = PythonOperator(
        task_id="validate_raw",
        python_callable=validate_raw_main,
    )

    bronze_orders = GlueJobOperator(
        task_id="run_glue_job_l1_orders",
        job_name="retail-bronze-orders",
        wait_for_completion=True,
    )

    bronze_customers = GlueJobOperator(
        task_id="run_glue_job_l1_customers",
        job_name="retail-bronze-customers",
        wait_for_completion=True,
    )

    bronze_products = GlueJobOperator(
        task_id="run_glue_job_l1_products",
        job_name="retail-bronze-products",
        wait_for_completion=True,
    )

    bronze_stores = GlueJobOperator(
        task_id="run_glue_job_l1_stores",
        job_name="retail-bronze-stores",
        wait_for_completion=True,
    )

    bronze_order_items = GlueJobOperator(
        task_id="run_glue_job_l1_order_items",
        job_name="retail-bronze-order-items",
        wait_for_completion=True,
    )

    bronze_payments = GlueJobOperator(
        task_id="run_glue_job_l1_payments",
        job_name="retail-bronze-payments",
        wait_for_completion=True,
    )

    silver = GlueJobOperator(
        task_id="run_glue_job_l2_transformations",
        job_name="retail-silver-job",
        wait_for_completion=True,
    )

    upload_to_snowflake_stage = PythonOperator(
        task_id="upload_to_snowflake_stage",
        python_callable=load_to_snowflake_stage_main,
        op_kwargs={"batch_date": "{{ ds }}"},
    )

    copy_into_snowflake = SnowflakeOperator(
        task_id="copy_into_snowflake",
        snowflake_conn_id="snowflake_default",
        sql="snowflake/sql/copy_into.sql",
    )

    dbt_run_test = BashOperator(
        task_id="dbt_run_test",
        bash_command="""
        bash /opt/airflow/scripts/run_dbt.sh
        """,
    )

    end = EmptyOperator(task_id="end")

    start >> [extract_postgres, extract_currency_api]

    [extract_postgres, extract_currency_api] >> land_raw_s3

    land_raw_s3 >> validate_raw

    validate_raw >> [
        bronze_orders,
        bronze_customers,
        bronze_products,
        bronze_stores,
        bronze_order_items,
        bronze_payments,
    ]

    [
        bronze_orders,
        bronze_customers,
        bronze_products,
        bronze_stores,
        bronze_order_items,
        bronze_payments,
    ] >> silver

    silver >> upload_to_snowflake_stage

    upload_to_snowflake_stage >> copy_into_snowflake

    copy_into_snowflake >> dbt_run_test

    dbt_run_test >> end
