import sys
sys.path.append("/opt/airflow/scripts")
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta
from dc_notify import notify_discord

default_args = {
    "owner": "data_engineer",
    "start_date": datetime(2025, 7, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_discord,
}

with DAG(
    dag_id="data_mart_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    template_searchpath=["/opt/airflow"],
    description="Daily ETL from DWH to Data Mart in BigQuery",
) as dag:

    load_total_revenue = BigQueryInsertJobOperator(
        task_id="load_total_revenue",
        configuration={
            "query": {
                "query": "{% include 'sql/total_revenue.sql' %}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "purwadika",
                    "datasetId": "jcdeol3_final_project_riki",
                    "tableId": "total_revenue",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
                "parameterMode": "NAMED",
            }
        },
        params={"full_load": "{{ dag_run.conf.get('full_load', False) }}"},
        location="asia-southeast2",
    )

    load_top_selling_products = BigQueryInsertJobOperator(
        task_id="load_top_selling_products",
        configuration={
            "query": {
                "query": "{% include 'sql/top_selling_products.sql' %}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "purwadika",
                    "datasetId": "jcdeol3_final_project_riki",
                    "tableId": "top_selling_products",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
                "parameterMode": "NAMED",
            }
        },
        params={"full_load": "{{ dag_run.conf.get('full_load', False) }}"},
        location="asia-southeast2",
    )

    load_rating_perstore = BigQueryInsertJobOperator(
        task_id="load_rating_perstore",
        configuration={
            "query": {
                "query": "{% include 'sql/rating_perstore.sql' %}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "purwadika",
                    "datasetId": "jcdeol3_final_project_riki",
                    "tableId": "rating_perstore",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
                "parameterMode": "NAMED",
            }
        },
        params={"full_load": "{{ dag_run.conf.get('full_load', False) }}"},
        location="asia-southeast2",
    )

    load_avg_price = BigQueryInsertJobOperator(
        task_id="load_avg_price",
        configuration={
            "query": {
                "query": "{% include 'sql/avg_price.sql' %}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "purwadika",
                    "datasetId": "jcdeol3_final_project_riki",
                    "tableId": "avg_price",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
                "parameterMode": "NAMED",
            }
        },
        params={"full_load": "{{ dag_run.conf.get('full_load', False) }}"},
        location="asia-southeast2",
    )

    load_total_revenue_perbulan = BigQueryInsertJobOperator(
        task_id="load_total_revenue_perbulan",
        configuration={
            "query": {
                "query": "{% include 'sql/revenue_perbulan.sql' %}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "purwadika",
                    "datasetId": "jcdeol3_final_project_riki",
                    "tableId": "revenue_perbulan",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
                "parameterMode": "NAMED",
            }
        },
        params={"full_load": "{{ dag_run.conf.get('full_load', False) }}"},
        location="asia-southeast2",
    )

    load_rating_perstore_perbulan = BigQueryInsertJobOperator(
        task_id="load_rating_perstore_perbulan",
        configuration={
            "query": {
                "query": "{% include 'sql/avg_rating_perbulan.sql' %}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "purwadika",
                    "datasetId": "jcdeol3_final_project_riki",
                    "tableId": "rating_perbulan",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
                "parameterMode": "NAMED",
            }
        },
        params={"full_load": "{{ dag_run.conf.get('full_load', False) }}"},
        location="asia-southeast2",
    )

    (
        load_total_revenue
        >> load_top_selling_products
        >> load_rating_perstore
        >> load_avg_price
        >> load_total_revenue_perbulan
        >> load_rating_perstore_perbulan
    )
