from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaProducer
from airflow.hooks.http_hook import HttpHook

default_args = {
    'owner': 'your_owner',
    'start_date': datetime(2023, 10, 11),
    'retries': 1,
    "provide_context":True, # 주의
}

dag = DAG(
    'fetch_seoul_air_data',
    default_args=default_args,
    #schedule_interval=timedelta(minutes=120),
    schedule_interval="30 1,3,5,7,9,11,13,15,17,19,21,23 * * *",  # 홀수 시의 30분마다 실행
    catchup=False,
)

fetch_data_task = SimpleHttpOperator(
    task_id='fetch_seoul_air_data',
    method='GET',
    endpoint='/7558457878736b7933374f6544556b/json/airPolutionMeasuring1Hour/1/999/',
    headers={'Content-Type': 'application/json'},
    http_conn_id='seoul', 
    response_check=lambda response: response.status_code == 200,
    dag=dag,
)

def produce_response(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_seoul_air_data')
    
    producer = KafkaProducer(
        bootstrap_servers=['172.31.9.242:19092','172.31.9.242:29092','172.31.9.242:39092'],
        key_serializer=None,
        value_serializer=lambda x: str(x).encode('utf-8'),  # 메시지 값을 문자열로 인코딩
        api_version=(0, 11, 5)
    )
    
    topic = 'kfk-test'
    
    print(f"SEND TO {topic}")
    
    value2 = data
    producer.send(topic, value=value2)
    producer.flush()
    
    print("SENDED")

print_task = PythonOperator(
    task_id='produce_response_data',
    provide_context=True,
    python_callable=produce_response,
    dag=dag,
)

fetch_data_task >> print_task
