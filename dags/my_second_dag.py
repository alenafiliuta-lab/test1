from airflow.sdk import task, dag
from airflow.operators.python import PythonOperator


@dag
def my_second_dag():
    @task
    def my_task():
        print("hello, world")

    @task
    def my_task2():
        print("hello, world2")

    @task
    def my_task3():
        print("hello, world3")

    my_task() >> my_task2() >> my_task3()


my_second_dag()
