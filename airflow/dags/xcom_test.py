from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Airflow DAG 정의
default_args = {
    'owner': 'team09_airflow',
    'start_date': datetime(2023, 10, 11),
    'retries': 1,
    'provide_context': True,
}

dag = DAG(
    'xcom-test',
    default_args=default_args,
    schedule_interval=timedelta(minutes=10),
    catchup=False,
)

def xcom_test(**context):
    return "xcom test!"

def xcom_test1(**context):
    text = context['task_instance'].xcom_pull(task_ids='xcom_test')
    return text

xcom_test = PythonOperator(
    task_id='xcom_test',
    python_callable=xcom_test,
    dag=dag,
    provide_context=True
)

xcom_test1 = PythonOperator(
    task_id='xcom_test1',
    python_callable=xcom_test1,
    dag=dag,
    ###provide_context=True
)

xcom_test >> xcom_test1
