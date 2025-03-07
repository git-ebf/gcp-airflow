from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def tarefa_exemplo():
    print("Execução de exemplo.")


with DAG(
    "exemplo_dag", start_date=datetime(2023, 1, 1), schedule_interval="@daily"
) as dag:
    exemplo = PythonOperator(task_id="exemplo_task", python_callable=tarefa_exemplo)
