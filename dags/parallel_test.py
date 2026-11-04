from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time
import os

def heavy_computation(task_number):
    print(f"Воркер начал задачу {task_number}")
    # Имитируем бурную деятельность
    time.sleep(30)
    print(f"Воркер закончил задачу {task_number}")
    return f"Done {task_number}"

with DAG(
    dag_id="celery_parallel_demo",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    for i in range(1, 6):
        PythonOperator(
            task_id=f"processing_chunk_{i}",
            python_callable=heavy_computation,
            op_kwargs={"task_number": i}
        )
