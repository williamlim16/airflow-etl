from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import pandas as pd

tmpl_search_path = Variable.get("csv_path")


dag = DAG(
    'test2',
    description='descrption',
    start_date=datetime(2023,12,19,2),
    catchup=False,
    schedule_interval=timedelta(days=1)
)

def run_query():
    query = pd.read_csv(tmpl_search_path)

tas = PythonOperator(
    task_id='run_query', dag=dag, python_callable=run_query)