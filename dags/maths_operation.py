from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def start_number(**context):
    context['ti'].xcom_push(key='current_value',value=10)
    print("Starting numbet 10")

def add_five(**context):
    current_value = context["ti"].xcom_pull(key="current_value", task_ids="start_task")
    new_value = current_value + 5
    context['ti'].xcom_push(key='current_value',value=new_value)
    print(f"Adding 5 to {current_value} to get {new_value}")

def multiply_by_two(**context):
    current_value = context["ti"].xcom_pull(key="current_value", task_ids="add_task")
    new_value = current_value * 2
    context['ti'].xcom_push(key='current_value',value=new_value)
    print(f"Multiplying {current_value} by 2 to get {new_value}")

def subtract_three(**context):
    current_value = context["ti"].xcom_pull(key="current_value", task_ids="multiply_task")
    new_value = current_value - 3
    context['ti'].xcom_push(key='current_value',value=new_value)
    print(f"Subtracting 3 from {current_value} to get {new_value}")


def square_number(**context):
    current_value = context["ti"].xcom_pull(key="current_value", task_ids="subtract_task")
    new_value = current_value ** 2
    context['ti'].xcom_push(key='current_value',value=new_value)
    print(f"Squaring {current_value} to get {new_value}")

with DAG(dag_id="math_operations",
         schedule = "@daily",
         start_date=datetime(2025, 1, 1),
         ) as dag:
    ## Define the task
    start_task = PythonOperator(task_id="start_task", python_callable=start_number)
    add_task = PythonOperator(task_id="add_task", python_callable=add_five)
    multiply_task = PythonOperator(task_id="multiply_task", python_callable=multiply_by_two)
    subtract_task = PythonOperator(task_id="subtract_task", python_callable=subtract_three)
    square_task = PythonOperator(task_id="square_task", python_callable=square_number)

    ## Dependencies
    start_task >> add_task >> multiply_task >> subtract_task >> square_task