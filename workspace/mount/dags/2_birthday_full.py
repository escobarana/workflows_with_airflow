from airflow import DAG
from airflow.operators.bash import BashOperator
from pendulum import datetime
import datetime as dt
from dateutil import relativedelta

"""
Exercise 2.5

Extend your previous result to also print your age.
"""


MY_NAME = "Barack Obama"
MY_BIRTHDAY = dt.datetime(year=1961, month=8, day=4)

dag = DAG(
    dag_id="happy_birthday_v2",
    description="Wishes you a happy birthday",
    default_args={"owner": "Airflow"},
    schedule_interval="0 0 4 8 *",
    start_date=MY_BIRTHDAY,
)

def years_today():
    """Returns how old you are at this moment"""
    return (
        "{{ macros.dateutil.relativedelta.relativedelta("
        "data_interval_end, "
        "dag.start_date"
        ").years }}"
    )


with dag:
    birthday_greeting = BashOperator(
        task_id="send_wishes",
        dag=dag,
        bash_command=(
            f"echo 'Happy birthday, {MY_NAME}! "
            f"You are {years_today()} years old today!'"
        ),
    )
