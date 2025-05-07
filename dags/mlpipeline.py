from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


# Define our task 1
def preprocess_data():
    print("Preprocessing data...")

## Define our task 2
def train_model():
    print("Training model...")

## Define our task 3
def evaluate_model():
    print("Evaluating model...")

## Define the DAG

with DAG(dag_id="ml_pipeline",
         schedule="@daily",
         start_date=datetime(2025, 1, 1),
         ) as dag:
    preprocess = PythonOperator(task_id="preprocess_task",python_callable=preprocess_data)
    train = PythonOperator(task_id="train_task",python_callable=train_model)
    evaluate = PythonOperator(task_id="evaluate_task",python_callable=evaluate_model)


    # Set task dependencies
    preprocess >> train >> evaluate
    