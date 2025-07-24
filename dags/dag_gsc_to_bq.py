import sys
sys.path.append("/opt/airflow/scripts")

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from gcs_to_bq import load_to_bq
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
BQ_DATASET = 'finpro_riki'

with DAG(
    dag_id='gcs_to_bq',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Load data from GCS to BigQuery',
    tags=['gcs', 'bq', 'load']
) as dag:

    previous_task = None

    for table_name, config in TABLE_CONFIG.items():
        load = PythonOperator(
            task_id=f'load_{table_name}_to_bq',
            python_callable=load_to_bq,
            op_kwargs={
                'table_name': table_name,
                'partition_column': config['created_column'],
                'bucket_name': BUCKET_NAME,
                'dataset_name': BQ_DATASET,
                'execution_date': "{{ execution_date }}",
                'full_load': True 
            }
        )
        if previous_task:
            previous_task >> load
        previous_task = load

    trigger_dag = TriggerDagRunOperator(
        task_id="trigger_transform_dag",
        trigger_dag_id="stg_to_dim_fact",
    )
    previous_task >> trigger_dag

