import sys
sys.path.append("/opt/airflow/scripts")

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
from datetime import datetime
from load_to_postgres import load_to_postgres
from dc_notify import notify_discord

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    'on_failure_callback': notify_discord
}

POSTGRES_CONN_ID = "postgres_default"


def get_postgres_conn_str():
    conn = BaseHook.get_connection(POSTGRES_CONN_ID)
    return f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"


with DAG(
    dag_id="load_csv",
    default_args=default_args,
    description="Incremental load CSV into PostgreSQL every hour",
    schedule_interval="@hourly",
    catchup=False,
) as dag:

    def wrap_load(**context):
        task_conf = context["params"]
        conn_str = get_postgres_conn_str()
        full_load = context["dag_run"].conf.get("full_load", True)

        execution_date = context.get("data_interval_start")
        next_execution_date = context.get("data_interval_end")

        if full_load:
            execution_date = None
            next_execution_date = None

        load_to_postgres(
            file_path=task_conf["file_path"],
            table_name=task_conf["table_name"],
            conn_str=conn_str,
            date_column=task_conf["date_column"],
            execution_date=execution_date,
            next_execution_date=next_execution_date,
            full_load=full_load,
        )

    load_store = PythonOperator(
        task_id="load_store",
        python_callable=wrap_load,
        provide_context=True,
        params={
            "file_path": "/opt/airflow/data/store.csv",
            "table_name": "store",
            "date_column": "created_at",
        },
    )

    load_product = PythonOperator(
        task_id="load_product",
        python_callable=wrap_load,
        provide_context=True,
        params={
            "file_path": "/opt/airflow/data/product.csv",
            "table_name": "product",
            "date_column": "created_at",
        },
    )

    load_sales = PythonOperator(
        task_id="load_sales",
        python_callable=wrap_load,
        provide_context=True,
        params={
            "file_path": "/opt/airflow/data/sales.csv",
            "table_name": "sales",
            "date_column": "transaction_date",
        },
    )
    load_rating = PythonOperator(
        task_id="load_rating",
        python_callable=wrap_load,
        provide_context=True,
        params={
            "file_path": "/opt/airflow/data/ratings.csv",
            "table_name": "rating",
            "date_column": "created_at",
        },
    )
    

    load_store >> load_product >> load_sales >> load_rating
