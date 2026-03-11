from airflow.sdk import dag, task
from datetime import datetime

@dag(
    start_date=datetime(2025, 3, 10),
    schedule=None,
    tags=["dynamic_task_demo"]

)

def dynamic_task_example():
    @task
    def get_files():
        return ["file1.csv", "file2.csv", "file3.csv"]
    
    @task
    def process_file(file_name):
        print(f"Processing {file_name}")

    files = get_files()
    process_file.expand(file_name=files)

dynamic_task_example()