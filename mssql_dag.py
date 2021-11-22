from airflow import DAG
from airflow.operators.mssql_operator import MsSqlOperator    

dag = DAG(
    'helloworld_dag',
    default_args=default_args,
    description='simplest DAG example',
    schedule_interval="0 14 * * *",
    catchup=False,
    dagrun_timeout=timedelta(minutes=60))

hit_mssql = MsSqlOperator(
         task_id='sql-op',
         mssql_conn_id='conn_dp_staging_hom',
         sql=f"SELECT @@VERSION",            
         autocommit=True,
         database='STAGING_HOM',
         dag=dag
     )
