import requests
from airflow.models import Variable

def notify_discord(context):
    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url
    try_number = context.get('task_instance').try_number

    message = f"""🚨 **Airflow Task Failed**
**DAG**: `{dag_id}`
**Task**: `{task_id}`
**Try**: `{try_number}`
**Execution Time**: `{execution_date}`
[View Logs]({log_url})
    """

    webhook_url = Variable.get("DISCORD_WEBHOOK_URL") # Ganti dengan webhook kamu
    requests.post(webhook_url, json={"content": message})
