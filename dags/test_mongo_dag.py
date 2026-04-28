from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime


def test_mongo():
    hook = MongoHook(mongo_conn_id='mongo_default')
    client = hook.get_conn()
    db = client['test_db']
    collection = db['test_collection']
    doc = {"test": "Hello from Airflow", "timestamp": datetime.now().isoformat()}
    collection.insert_one(doc)
    print(f"Inserted: {doc}")

with DAG(
    dag_id='test_mongo_connection',
    schedule=None,
    start_date=datetime(2026, 3, 30),
    catchup=False,
) as dag:
    test_task = PythonOperator(
        task_id='insert_document',
        python_callable=test_mongo,
    )