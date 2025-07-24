import sys
sys.path.append("/opt/airflow/scripts")
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pg_to_gcs import extract_to_gcs
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dc_notify import notify_discord

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': notify_discord
}

TABLE_CONFIG = {
    'store': {'created_column': 'created_at'},
    'product': {'created_column': 'created_at'},
    'rating': {'created_column': 'created_at'},
    'sales': {'created_column': 'transaction_date'}
}

BUCKET_NAME = 'jdeol003-bucket'
BQ_DATASET = "finpro_riki"

with DAG(
    dag_id='pg_to_gcs',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Extract incremental data from Postgres to GCS',
    tags=['postgres', 'gcs', 'extract']
) as dag:

    previous_task = None

    for table_name, config in TABLE_CONFIG.items():
        load = PythonOperator(
            task_id=f'full_load_{table_name}_to_gcs',
            python_callable=extract_to_gcs,
            op_kwargs={
                'table_name': table_name,
                'created_column': config['created_column'],
                'bucket_name': BUCKET_NAME,
                'dataset_name': BQ_DATASET,
                'full_load': True
            }
        )
        if previous_task:
            previous_task >> load
        previous_task = load

    trigger_dag = TriggerDagRunOperator(
        task_id="trigger_transform_dag",
        trigger_dag_id="gcs_to_bq",
    )
    previous_task >> trigger_dag

