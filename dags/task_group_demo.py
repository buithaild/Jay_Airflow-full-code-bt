from airflow.sdk import dag, task
from datetime import datetime
from  airflow.utils.task_group import TaskGroup

@dag(
    start_date=datetime(2026, 3, 10),
    schedule=None,
    tags= ["group_tasks"]
)

def task_group_eaxample():
    with TaskGroup("extract_tasks") as extract:

        @task
        def extract_users():
            print("Extracting User")

        @task
        def extract_orders():
            print("Extracting Orders")

        extract_users()
        extract_orders()

    @task
    def load():
        print("loading data")

    
    load_task = load()

    extract >> load_task

task_group_eaxample()