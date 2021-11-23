from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'Sergio',
    'start_date': datetime(2020, 5, 5),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

BASE_FOLDER = "/opt/airflow/dags/repo/lake/{stage}/{partition}"

PARTITION_FOLDER = "extract_date={{ ds }}"

with DAG('bash_dag',
         default_args=default_args,
         schedule_interval=None) as dag:


    bash_task = BashOperator(task_id='bash_task', 
                             bash_command="echo 'Time de Dados! Rente, Amanda, Gabs, Presunto e Sérgio'")
