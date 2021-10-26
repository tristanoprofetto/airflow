try:

    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    print('All good')

except Exception as e:
    print("Error {}".format(e))



def t1(**context):
    print("t1 ")
    context['ti'].xcom_push(key='mykey', value="t1")


def t2(**context):
    instance = context.get("ti").xcom_pull(key="mykey")
    data = [{"name":"Tris","title":"Data Science"}, { "name":"King","title":"Product Designer"},]
    df = pd.DataFrame(data=data)
    print('@'*66)
    print(df.head())
    print('@'*66)

    print("I am in t2 got value :{} from Function 1  ".format(instance))



with DAG(
    dag_id="dag",
    schedule_interval="@daily",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2021,1,1),
    },
    catchup=False) as f:

    t1 = PythonOperator(
    task_id="t1",
    python_callable=t1,
    provide_context=True,
    op_kwargs={"name":"Soumil"}
    )

    t2 = PythonOperator(
        task_id="t1",
        python_callable=t2,
        provide_context=True,
    )

t1 >> t2 

