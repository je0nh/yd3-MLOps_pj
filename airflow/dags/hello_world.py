from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 3, 31),
    'email': ['abc@abc.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    dag_id='my_dag',
    catchup=False,
    default_args=default_args,
    schedule_interval=timedelta(minutes=1),  # 매 분마다 실행
)

def print_time():
    now = datetime.now()
    print('=' * 20)
    print('현재 시간은 {}입니다.'.format(now))
    print('=' * 20)
    with open('/tmp/current_time.txt', 'a') as f:
        f.write('현재 시간은 {}입니다.\n'.format(now))

t1 = PythonOperator(
    task_id='print_time',
    python_callable=print_time,
    dag=dag,
)

t1
