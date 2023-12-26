from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator

default_args ={
  'owner': 'william',
  'retries': 5,
  'retry_delay': timedelta(minutes=2),
}

with DAG(
  dag_id='test_dag',
  default_args=default_args,
  description='descrption',
  start_date=datetime(2023,12,19,2),
  schedule_interval='@daily'
) as dag:
  task1 = BashOperator(
    task_id = 'first_task',
    bash_command='echo hello world',
  )
  task1