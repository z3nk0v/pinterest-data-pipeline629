# Databricks notebook source
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta 


#Define params for Submit Run Operator
notebook_task = {
    'notebook_path': 'Users/dimacr98@gmail.com/DAG',
}


#Define params for Run Now Operator
notebook_params = {
    "Variable":5
}


default_args = {
    'owner': 'Dmitry',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': '2',
    'retry_delay': timedelta(minutes=2)
}


with DAG('0ea7b76ff169_dag',
    # should be a datetime format
    start_date=datetime(2023, 5, 4),
    # check out possible intervals, should be a string
    schedule_interval='*/1 * * * *',
    catchup=False,
    default_args=default_args
    ) as dag:


    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        # the connection we set-up previously
        databricks_conn_id='databricks_default',
        existing_cluster_id='',
        notebook_task=notebook_task
    )
    opr_submit_run