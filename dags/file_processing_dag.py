from airflow.sdk import dag, task, task_group
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.bash import BashOperator
import pandas as pd
import os
from datetime import datetime
from airflow.sdk.definitions.asset import Asset


my_asset = Asset(uri="file:///opt/airflow/data/cleaned_reviews.csv", name="cleaned_reviews")
filepath = '/opt/airflow/data/tiktok_google_play_reviews.csv'
TEMP_FILE_1 = '/opt/airflow/data/filledna.csv'
TEMP_FILE_2 = '/opt/airflow/data/sorted.csv'
OUTPUT_FILE = '/opt/airflow/data/cleaned_reviews.csv'

@dag(dag_id='file_processing_dag',
    schedule=None,
    start_date=datetime(2026, 3, 29),
)
def file_processing_dag():
    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath='/opt/airflow/data/tiktok_google_play_reviews.csv',
        fs_conn_id='fs_default',
        timeout=60 * 60 * 2,
        poke_interval=60,
        mode='poke',
    )

    @task.branch
    def check_file_status():
        file_path = '/opt/airflow/data/tiktok_google_play_reviews.csv'
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            return 'process_data'
        else:
            return 'log_empty_file'

    log_empty_file = BashOperator(
        task_id='log_empty_file',
        bash_command='echo "Файл {{ params.filepath }} пуст. Обработка не требуется." >> /opt/airflow/logs/airflow_log.txt',
        params={'filepath': '/opt/airflow/data/tiktok_google_play_reviews.csv'}
        )

    @task_group(group_id='process_data')
    def process_data():
        @task
        def fillna()-> str:
           df = pd.read_csv(filepath)
           df= df.fillna('-')
           df.to_csv(TEMP_FILE_1, index=False)
           return TEMP_FILE_1
        @task
        def sorted_date():
            df = pd.read_csv(TEMP_FILE_1)
            df = df.sort_values(by=['at'], ascending=False)
            df.to_csv(TEMP_FILE_2, index=False)
            return TEMP_FILE_2
        @task(outlets=[my_asset])
        def replace():
            df=pd.read_csv(TEMP_FILE_2)
            df['content']=df['content'].str.replace(r'[^\w\s.,!?-]', '', regex=True)
            df.to_csv(OUTPUT_FILE, index=False)
            return OUTPUT_FILE

        fillna()>>sorted_date()>>replace()


    wait_for_file >>check_file_status()>>[process_data(), log_empty_file]


file_processing_dag()

