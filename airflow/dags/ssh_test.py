from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator

default_args = {
    'owner': 'team09',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 10, 3, 0), # 시작 날짜 및 시간 설정
    'retries': 2
}

dag = DAG(
    'hadoop_master1',
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
    catchup=False
)

# SSHHook 객체 생성
ssh_hook = SSHHook(
    ssh_conn_id="hadoop_master1",
    username="ubuntu",
    remote_host="3.39.211.131",
    key_file="/home/airflow/.ssh/id_rsa",
    conn_timeout=9999999999,
    cmd_timeout=99999
)

#with DAG('ssh_test',
#         default_args=default_args,
#         schedule_interval='0 3 * * *', # 스케줄 (매일 3시에 실행)
#         catchup=False
#         ) as dag:
    # SSH Hook 설정
#    ssh_hook = SSHHook(ssh_conn_id="hadoop_master1")
    # SSHOperator를 이용하여 실행할 파일 경로 설정
    #remote_file_path = "/home/ubuntu/api-data/kafka-cons-pollution.py"
    # SSHOperator를 이용하여 파일 실행
#    run_file_command = "python3 /home/ubuntu/api-data/kafka-cons-test.py"
    #run_file_command = "echo 'hi'"
#    ssh_operator = SSHOperator(task_id="run_file", ssh_hook=ssh_hook, command=run_file_command)
    
kfk_cons = SSHOperator(
    task_id='kfk_cons',
    ssh_hook=ssh_hook,
    command="python3 /home/ubuntu/api-data/kafka-cons-pollution.py",
    execution_timeout=timedelta(minutes=30),
    dag=dag
)
