from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import fire

# Airflow DAG 정의
default_args = {
    'owner': 'team09_airflow',
    'start_date': datetime(2023, 10, 11),
    'retries': 1,
    'provide_context': True,
}

dag = DAG(
    'call_api_by_fire',
    default_args=default_args,
    #schedule_interval=timedelta(minutes=120),
    schedule_interval="30 0,2,4,6,8,10,12,14,16,18,20,22 * * *",  # 홀수 시의 30분마다 실행???
    catchup=False,
)

def run_api_pollution():
    from subprocess import call
    print("run_api_pollution")
    call(["python", "/opt/airflow/dags/api_pollution.py", "load_pollution_data", "--param1", "", "--param2", ""])
    
def run_api_traffic():
    from subprocess import call
    call(["python", "/opt/airflow/dags/api_traffic.py", "load_traffic_data", "--param1", "", "--param2", ""])


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

# 작업 간의 관계 설정
call_api_pollution >> call_api_traffic
