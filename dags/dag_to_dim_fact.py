import sys
sys.path.append("/opt/airflow/scripts")
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dc_notify import notify_discord

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2025, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': notify_discord
}

with DAG(
    dag_id='stg_to_dim_fact',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    template_searchpath=['/opt/airflow'],
    description='Daily ETL from staging to DWH in BigQuery',
) as dag:

    load_dim_store = BigQueryInsertJobOperator(
        task_id='load_dim_store',
        configuration={
            "query": {
                "query": "{% include 'sql/dim_store.sql' %}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "purwadika",
                    "datasetId": "jcdeol3_final_project_riki",
                    "tableId": "dim_store"
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
                "timePartitioning": {"type": "DAY", "field": "created_at"},
                "parameterMode": "NAMED"
            }
        },
        params={"full_load": "{{ dag_run.conf.get('full_load', False) }}"},
        location='asia-southeast2',
    )

    load_dim_product = BigQueryInsertJobOperator(
        task_id='load_dim_product',
        configuration={
            "query": {
                "query": "{% include 'sql/dim_product.sql' %}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "purwadika",
                    "datasetId": "jcdeol3_final_project_riki",
                    "tableId": "dim_product"
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
                "timePartitioning": {"type": "DAY", "field": "created_at"},
                "parameterMode": "NAMED"
            }
        },
        params={"full_load": "{{ dag_run.conf.get('full_load', False) }}"},
        location='asia-southeast2',
    )
    load_dim_rating = BigQueryInsertJobOperator(
        task_id='load_dim_rating',
        configuration={
            "query": {
                "query": "{% include 'sql/dim_rating.sql' %}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "purwadika",
                    "datasetId": "jcdeol3_final_project_riki",
                    "tableId": "dim_rating"
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
                "timePartitioning": {"type": "DAY", "field": "created_at"},
                "parameterMode": "NAMED"
            }
        },
        params={"full_load": "{{ dag_run.conf.get('full_load', False) }}"},
        location='asia-southeast2',
    )

    load_fact_sales = BigQueryInsertJobOperator(
        task_id='load_fact_sales',
        configuration={
            "query": {
                "query": "{% include 'sql/fact_sales.sql' %}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "purwadika",
                    "datasetId": "jcdeol3_final_project_riki",
                    "tableId": "fact_sales"
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
                "timePartitioning": {"type": "DAY", "field": "transaction_date"},
                "parameterMode": "NAMED"
            }
        },
        params={"full_load": "{{ dag_run.conf.get('full_load', False) }}"},
        location='asia-southeast2',
    )
    trigger_dag = TriggerDagRunOperator(
        task_id="trigger_transform_dag",
        trigger_dag_id="data_mart_dag",
    )
    load_dim_store >> load_dim_product >> load_dim_rating >> load_fact_sales >> trigger_dag
