from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": 30, 
}

def start_task():
    print("Pipeline started.")

def extract_data():
    print("Extracting data...")
    print("Data extracted successfully.")

def transform_data():
    print("Transforming Data")
    raise Exception("Intentional failure for UI demo")
    print("Data transformed successfully.")

def load_data():
    print("Loading data...")
    print("Data loaded successfully.")

with DAG(
    dag_id="ui_demo_pipeline",
    description="DAG for Airflow UI walkthrough demo",
    default_args=default_args,
    start_date=datetime(2026, 3, 11),
    schedule="*/1 * * * *", # 30 10,22 * * 1,2: 10h30 va 22h30 thu 2 va thu 3 hang tuan; # 30 10,22 * * 1-2: 10h30 va 22h30 thu 2 den thu 6; */5 * * * *: 5 phut chay 1 lan
    #0 0 2 * * : ngay thu 2 cua moi thang
    #0 0 1,2 * * : ngay thu 1 va thu 2 cua moi thang
    # "@daily": moi ngay
    #"0 0 11-20 * *" tu ngay 11 den 20
    catchup=False,
    tags=["ui-demo", "training"],
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=start_task,
    )

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    load = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    extract >> transform >> load