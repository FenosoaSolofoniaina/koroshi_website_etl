from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

import sys
sys.path.append("/opt/scraper")
from main import say_hello


def print_message() :

    print("Simple message")



default_args =  {
    "start_date" : datetime.now(),
    "schedule" : None,
    "retries" : 3,
    "retry_delay" : timedelta(minutes=2),
    "catchup" : False
}

with DAG(
    dag_id="test_dag_v1",
    description="This is a dag test",
    default_args=default_args
) as dag :
    
    task_1 = PythonOperator(task_id="Task_1", python_callable=print_message)

    task_2 = PythonOperator(task_id="Task_2", python_callable=say_hello)

    task_1 >> task_2