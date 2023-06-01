from datetime import timedelta, datetime
import airflow
import pandas as pd
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

def send_failure_notification(context):
    import pymsteams
    myTeamsMessage = pymsteams.connectorcard("https://anbinhsc.webhook.office.com/webhookb2/3be31bb2-7099-4afa-b659-e04ad1bd4435@7ec05515-0732-4ea8-937f-02fc55d0f05b/IncomingWebhook/5209d65defcc4bb6a4f83773581916a1/4831c6f5-606b-4f4a-ac5e-0cc3c17fdd8c")
    body = f"{context['task_instance'].task_id} has failed."
    myTeamsMessage.text(body)
    myTeamsMessage.send()
    
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 31),
    'on_failure_callback': send_failure_notification
} 
    
dag = DAG(
    'etl_weekly_alert_notifications',
    default_args = default_args,
    schedule_interval = timedelta(days=1)
)


def extract():
    import sys
    sys.path.append('/opt/airflow/projects')
    from w_get_stock_alert_noti.extract import extract_vn
    
    extract_vn()
    
task_extract = PythonVirtualenvOperator(
    task_id="task_extract",
    requirements=['beautifulsoup4', 'numpy', 'pandas', 'requests'],
    python_callable=extract,
    dag=dag
)

    
def transform():
    import sys
    sys.path.append('/opt/airflow/projects')
    from w_get_stock_alert_noti.transform import transform_vn
    
    df = transform_vn()
    df.to_csv('data/w_get_stock_alert_noti/all_alerts.csv', index = False)
    
task_transform = PythonOperator(
    task_id='task_transform',
    python_callable=transform,
    dag = dag
)

truncate_task = PostgresOperator(
sql = 'truncate table extnl.temp_table',
task_id = "truncate_task",
postgres_conn_id = "dwh_connection",
dag = dag
)


def load():
    df = pd.read_csv('data/w_get_stock_alert_noti/all_alerts.csv')
    records = df.to_records(index=False)
    result = list(records)
    
    # Create a PostgresHook
    hook = PostgresHook(postgres_conn_id='dwh_connection')
    
    # Define the INSERT statement
    sql = """
        INSERT INTO extnl.temp_table(ticker, isin_code, figi_code, company_name, notification_date, effective_date, reason, alert_code, alert_language, etldatetime) 
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
    
    # Execute the INSERT statement for each row
    for row in result:
        hook.run(sql, parameters=row)
        
task_load = PythonOperator(
    task_id='task_load',
    python_callable=load,
    dag=dag
)


task_extract >> task_transform >> truncate_task >> task_load



