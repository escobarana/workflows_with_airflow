import datetime as dt
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.helpers import cross_downstream

"""
Exercise 3

This DAG contains a lot of repetitive, duplicated and ultimately boring code.
Can you simplify this DAG and make it more concise?
"""

dag = DAG(
    dag_id="repetitive_tasks",
    description="Many tasks in parallel",
    default_args={"owner": "Airflow"},
    schedule_interval="@daily",
    start_date=dt.datetime(2021, 1, 1),
    end_date=dt.datetime(2021, 1, 15),
)

def create_task(task_id: str) -> BashOperator:
    return BashOperator( task_id=task_id, dag=dag, bash_command="echo '{task_id} done'" )

initial_dags_list = [create_task(task_id="task_a"), create_task(task_id="task_b"), create_task(task_id="task_c"), create_task(task_id="task_d")]
final_dags_list = [create_task(task_id="task_e"), create_task(task_id="task_f"), create_task(task_id="task_g"), create_task(task_id="task_h")]
cross_downstream(initial_dags_list, final_dags_list)
