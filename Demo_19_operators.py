from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
import os 
import logging 
def main():
    logging.basicConfig(filename='myapp.log', level=logging.INFO)
    logging.info('Started')
    logging.info('Finished')
    if __name__ == '__main__':
        main()
def hello_world():
    logging.info("Hello world ")

def current_time():
    logging.info(f"Current time is {datetime.utcnow().isoformat()}")

def working_dir():
    logging.info(f"Working directory is {os.getcwd}")

def compelete():
    logging.info(f"Congrats, your first multi-task pipeline in now compelete! ")

with DAG("Demo19_operators_and_tasks",start_date=datetime(2021,1,1),schedule_interval="@daily",catchup=False) as dag:
    
    hello_world_task = PythonOperator(
    task_id = "hello_world",
    python_callable=hello_world)

    start = DummyOperator(task_id="start")

    working_directory_task  = PythonOperator(
    task_id = "working_directory",
    python_callable = working_dir)

    current_time_task  = PythonOperator(
    task_id = "current_time",
    python_callable = current_time)

    compelete_task = PythonOperator(
            task_id = "compelete",
            python_callable = compelete)


    start >> hello_world_task >> [working_directory_task,current_time_task] >> compelete_task 




