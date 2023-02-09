import datetime as dt
from airflow import DAG
from airflow.operators.bash import BashOperator

"""
Exercise 2

Create a DAG which will run on your birthday to congratulate you.
"""

MY_NAME = "Ana Escobar"
MY_BIRTHDAY = dt.datetime(year=1999, month=3, day=12)

dag = DAG(
    dag_id="happy_birthday_v1",
    description="Wishes you a happy birthday",
    default_args={"owner": "Airflow"},
    schedule_interval="0 0 12 3 *",
    start_date=MY_BIRTHDAY,
)

birthday_greeting = BashOperator(
    task_id="send_wishes",
    dag=dag,
    bash_command=f"echo 'Happy birthday, {MY_NAME}!'",
)
