#Import Essentials

from datetime import datetime
from airflow.sdk import dag
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)

#Define DAG
@dag(
    dag_id = "bash_operator_demo",
    start_date=datetime(2026,3,10),
    schedule="@daily",
    catchup=False,
    tags=["bashdemo"]
)

def bash_operator_demo():

    daily_sales_file_path = Variable.get("daily_sales_file_path")

    #1. Define File Sensor
    wait_for_csv = FileSensor(
        task_id="wait_for_daily_sales_csv",
        filepath=daily_sales_file_path,
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=360,
        mode="poke",
    )

    #2. Define task with Bash operator
    List_dag_files = BashOperator(
        task_id = "list_dag_folder_files",
        bash_command="echo 'Files in DAGs Folders:' && ls -lh /opt/airflow/dags"
    )

    #3. Cloud Operator - upload to GCS
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_csv_to_gcs",
        src=daily_sales_file_path, #"	/opt/airflow/dags/daily_sales.csv"
        dst="demo/daily_sales.csv",
        bucket="airflow-demo-gcs-thaibui",
        gcp_conn_id="google_cloud_default"
    )

    #Define Dependencies
    wait_for_csv >> List_dag_files >> upload_to_gcs

bash_operator_demo()