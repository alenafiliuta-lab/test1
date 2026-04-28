from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeCheckOperator

with DAG(dag_id='snowflake_connection') as dag:
    check_snowflake = SnowflakeCheckOperator(
        task_id='check_snowflake_connection',
        snowflake_conn_id='snowflake_conn_id',
        sql="SELECT 1",
    )