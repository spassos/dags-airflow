from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.mssql_operator import MsSqlOperator    

default_args = {
    'owner': 'Sergio',
    'start_date': datetime(2020, 5, 5),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('mssql_dag',
         default_args=default_args,
         schedule_interval=None) as dag:


    hit_mssql = MsSqlOperator(
             task_id='sql-op',
             mssql_conn_id='conn_dp_staging_hom',
             sql='SELECT @@VERSION',            
             autocommit=True,
             database='STAGING_HOM',
             dag=dag
         )
    
    hit_mssql
