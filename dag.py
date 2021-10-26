from airflow import DAG
from airflow.operators import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from random import randint
from datetime import datetime

def _training_model():
    return randint(1,10)


def _choose_best(ti):
    acc = ti.xcom_pull(task_ids=['training_A', 'training_B', 'training_C'])
    best_accuracy = max(acc)
    if best_accuracy > 8:
        return 'accurate'
    else:
        return 'innacurate'


with DAG(
    id="dag", 
    start_date=datetime(2021,1,1), 
    schedule_interval="@daily",
    catchup=False
    ) as dag:
        training_A = PythonOperator(
            task_id="training_A",
            python_callable=_training_model
        )
        training_B = PythonOperator(
            task_id="training_B",
            python_callable=_training_model
        )
        training_C = PythonOperator(
            task_id="training_C",
            python_callable=_training_model
        )
        choose_best = BranchPythonOperator(
            task_id="choose_best",
            python_callable=_choose_best
        )
        accurate = BashOperator(
            task_id="accurate",
            bash_command="echo 'accurate'"
        )
        innaccurate = BashOperator(
            task_id="accurate",
            bash_command="echo 'innaccurate'"
        )

        [training_A, training_B, training_C] >> choose_best >> [accurate, innaccurate]