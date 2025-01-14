import datetime as dt

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

"""
Exercise 5

This DAG needs to do an extra step on Saturday, 
but the current implementation has some downsides.

What's wrong? And how can you fix this?
"""


def on_failure_callback(**context):
    print("Fail works  !  And something went wrong :)")


dag = DAG(
    dag_id="aggregate_on_saturday",
    description="On saturdays we run aggregations",
    default_args={"owner": "Airflow"},
    schedule_interval="@daily",
    start_date=dt.datetime(2021, 1, 1),
    end_date=dt.datetime(2021, 1, 15),
)


def today_is_saturday():
    today = dt.date.today()
    return today.isoweekday() == 6


def create_task(name):
    return BashOperator(
        task_id=name,
        dag=dag,
        bash_command=f"echo '{name} done'",
    )

ingestion_task = create_task("ingestion")
cleaning_task = create_task("cleaning")
all_done = DummyOperator(task_id="all_done", dag=dag)

if today_is_saturday():
    aggregation_task = create_task("aggregation")
    
else:
    ingestion_task >> cleaning_task >> all_done


bracnh_for_saturday = BranchDayOfWeekOperatoºr(
    task_id = "",
    use_task_execution_date= False
)
ingestion_task >> cleaning_task >> bracnh_for_saturday >> [aggregation_task, all_done]
