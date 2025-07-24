import pandas as pd
from sqlalchemy import create_engine
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import os

def extract_to_gcs(
    table_name,
    created_column,
    bucket_name,
    dataset_name,
    gcp_conn_id="google_cloud_default",
    execution_date=None,
    full_load=False,  
):
    if execution_date is None:
        execution_date = datetime.today()

    if not full_load:
        # Ambil H-1
        target_date = (execution_date - timedelta(days=1)).date()
        start_time = datetime.combine(target_date, datetime.min.time())
        end_time = datetime.combine(target_date + timedelta(days=1), datetime.min.time())

   
    conn = BaseHook.get_connection("postgres_default")
    POSTGRES_CONN_STR = conn.get_uri()

    if POSTGRES_CONN_STR.startswith("postgres://"):
        POSTGRES_CONN_STR = POSTGRES_CONN_STR.replace("postgres://", "postgresql+psycopg2://", 1)

    if full_load:
        query = f"SELECT * FROM {table_name}"
        temp_path = f"/tmp/{table_name}_full_load.csv"
        gcs_path = f"{dataset_name}/{table_name}.csv"
    else:
        query = f"""
            SELECT * FROM {table_name}
            WHERE {created_column} >= '{start_time}'
              AND {created_column} < '{end_time}'
        """
        temp_path = f"/tmp/{table_name}_{target_date}.csv"
        gcs_path = f"{dataset_name}/{table_name}-{target_date}.csv"

    
    engine = create_engine(POSTGRES_CONN_STR)
    df = pd.read_sql_query(query, engine)
   
    df.to_csv(temp_path, index=False)
    
    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
    gcs_hook.upload(bucket_name=bucket_name, object_name=gcs_path, filename=temp_path, timeout=300)

    os.remove(temp_path)

    print(f"{'FULL LOAD' if full_load else 'INCREMENTAL LOAD'}: Uploaded {len(df)} rows from {table_name} to gs://{bucket_name}/{gcs_path}")
