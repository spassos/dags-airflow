from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.mssql_operator import MsSqlOperator    
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

default_args = {
    'owner': 'Sergio',
    'start_date': datetime(2020, 5, 5),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

BASE_FOLDER = "datalake/{stage}/{partition}"

PARTITION_FOLDER = "extract_date={{ ds }}"

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
    
    spark = SparkSubmitOperator(
             task_id="spark-test",
             conn_id="conn_spark_cluster",
             application="/opt/airflow/dags/spark/basic.py",
             name="basic",
             application_args=[
                 "--src",
                 BASE_FOLDER.format(stage="raw", partition=PARTITION_FOLDER),
                 "--dest",
                 BASE_FOLDER.format(stage="cleansed", partition=""),
                 "--process-date",
                 "{{ ds }}",
            ]
    )
    
    hit_mssql >> spark
