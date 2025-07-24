from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
from datetime import datetime, timedelta


def load_to_bq(
    table_name, partition_column, bucket_name, dataset_name, execution_date=None, full_load=False,
):
    
    if execution_date is None:
        execution_date = datetime.today()
    else:
        if isinstance(execution_date, str):
            execution_date = datetime.fromisoformat(execution_date)
    
    target_date = (execution_date - timedelta(days=1)).date()
    
    if full_load:
        gcs_filename = f"{table_name}.csv"
    else:
        gcs_filename = f"{table_name}-{target_date}.csv"


    gcs_uri = f"gs://{bucket_name}/{dataset_name}/{gcs_filename}"
    bq_table_id = f"purwadika.jcdeol3_final_project_riki.stg_{table_name}"

    hook = BigQueryHook(gcp_conn_id="google_cloud_default")
    client = hook.get_client()

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND" if not full_load else "WRITE_TRUNCATE",
        autodetect=True,
    )

    load_job = client.load_table_from_uri(gcs_uri, bq_table_id, job_config=job_config)
    load_job.result()


