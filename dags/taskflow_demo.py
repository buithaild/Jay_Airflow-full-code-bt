from datetime import datetime
from airflow.sdk import dag, task

@dag(
    schedule="@daily",
    start_date=datetime(year=2026, month=3, day=10),
    end_date=datetime(year=2026, month=3, day=31),
    catchup=True,
    tags=["training", "taskflow"]
)

def taskflow_demo():
    @task
    def extract():
        data = ["apple", "banana", "cherry"]
        return data
    
    @task
    def transform(data):
        transformed_data = [item.upper() for item in data]
        return transformed_data
    
    @task
    def load(data):
        for item in data:
            print(f"Loaded: item- {item}")

    load(transform(extract()))

taskflow_demo( )