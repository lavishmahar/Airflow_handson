from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG(dag_id = 'math_sequene_dag_with_taskflow',
         start_date = datetime(2025, 1, 1),
         schedule = '@daily',
         ) as dag:
    

    # Task 1 : Start with the initial number
    @task
    def start_number():
        initial_value = 10
        print(f"Initial value: {initial_value}")
        return initial_value
    
    # Task 2 : Add 5 to the number
    @task
    def add_five(number):
        result = number + 5
        print(f"After adding 5: {result}")
        return result
    
    # Task 3 : Multiply the number by 2
    @task
    def multiply_by_two(number):
        result = number * 2
        print(f"After multiplying by 2: {result}")
        return result
    
    # Task 4 : Subtract 3 from the number
    @task
    def subtract_three(number):
        result = number - 3
        print(f"After subtracting 3: {result}")
        return result
    
    # Square the number
    @task
    def square_number(number):
        result = number ** 2
        print(f"After squaring: {result}")
        return result
    
    # Set up the task dependencies
    start_value = start_number()
    added_value = add_five(start_value)
    multiplied_value = multiply_by_two(added_value)
    subtracted_value = subtract_three(multiplied_value)
    squared_value = square_number(subtracted_value)
