from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta

from api_pollution import PollutionProcessor
from api_traffic import TrafficProcessor


# Airflow DAG 정의
default_args = {
    'owner': 'team09_airflow',
    'start_date': datetime(2023, 10, 11),
    'retries': 1,
    'provide_context': True,
}

dag = DAG(
    'fetchdata_from_seoul_data',
    default_args=default_args,
    #schedule_interval=timedelta(minutes=120),
    schedule_interval="30 0,2,4,6,8,10,12,14,16,18,20,22 * * *",  # 홀수 시의 30분마다 실행???
    catchup=False,
)

#대기 데이터 api 호출
def run_api_pollution():
    #/opt/airflow/dags/api_pollution.py
    print("run_api_pollution")
    p = PollutionProcessor()
    pd = p.load_pollution_data()
    Variable.set("pd_val",pd)
    
#교통량 데이터 api 호출
def run_api_traffic():
    #/opt/airflow/dags/api_traffic.py
    print("run_api_traffic")
    t = TrafficProcessor()
    td = t.load_traffic_data()
    Variable.set("td_val",td)
    
#받은 데이터 카프카 토픽 전송
def send_to_topic():
    #py파일 직접 실행
    #from subprocess import call
    #call(["python", "/opt/airflow/dags/kafka_producer.py", "", "--param1", "", "--param2", ""])    
    pd = Variable.get("pd_val")
    print(pd)
    # 대기 데이터 전송 코드 Topic -> kfk-pollution
    
    
    td = Variable.get("td_val")
    print(td)
    # 교통량 데이터 전송 코드 Topic -> kfk-traffic


call_api_pollution = PythonOperator(
    task_id='call_api_pollution',
    python_callable=run_api_pollution,
    dag=dag,
)

call_api_traffic = PythonOperator(
    task_id='call_api_traffic',
    python_callable=run_api_traffic,
    dag=dag,
)

send_to_topic = PythonOperator(
    task_id='send_to_topic',
    python_callable=send_to_topic,
    dag=dag,
)

# 작업 간의 관계 설정
call_api_pollution >> call_api_traffic >> send_to_topic
