from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def extract(**context):
    data = ["apple", "banana", "cherry"]
    context["ti"].xcom_push(key="extracted_data", value=data)

def transform(**context):
    ti = context["ti"]
    data = ti.xcom_pull(
        task_ids="extract_task",
        key="extracted_data"
    )
    transformed_data = [item.upper() for item in data]
    ti.xcom_push(key="transformed_data", value=transformed_data)

def load(**context):
    ti = context["ti"]
    data = ti.xcom_pull(
        task_ids="transform_task",
        key="transformed_data"
    )
    for item in data:
        print(f"Loaded: {item}")

with DAG(
    dag_id="classic_api_xcom_demo",
    schedule="@daily",
    start_date=datetime(2025, 12, 18),
    catchup=False,
    tags=["training", "classic_api"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load,
    )

    extract_task >> transform_task >> load_task