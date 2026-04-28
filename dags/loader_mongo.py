from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime
from airflow.sdk.definitions.asset import Asset
import pandas as pd

my_asset = Asset(uri="file:///opt/airflow/data/cleaned_reviews.csv", name="cleaned_reviews")

def load_data_to_mongo():
    hook = MongoHook(mongo_conn_id='mongo_default')
    client = hook.get_conn()
    db = client['tictok_db']
    collection = db['tictok_collection']
    file_path = my_asset.uri.replace('file://', '')
    df = pd.read_csv(file_path)
    data = df.to_dict(orient='records')
    if not data:
        print("No data to insert")
        return
    collection.insert_many(data)
    print(f"Inserted {len(data)} documents")

with DAG(
    dag_id='ticktok_mongo_connection',
    schedule=[my_asset],
    start_date=datetime(2026, 3, 30),
    catchup=False,
) as dag:
    test_task = PythonOperator(
        task_id='insert_document',
        python_callable=load_data_to_mongo,
    )