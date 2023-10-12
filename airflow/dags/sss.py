import requests
import xml.etree.ElementTree as elemTree
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaProducer
from json import dumps
from airflow.hooks.http_hook import HttpHook
from airflow.providers.http.operators.http import SimpleHttpOperator
import json

# Airflow DAG 정의
default_args = {
    'owner': 'your_owner',
    'start_date': datetime(2023, 10, 11),
    'retries': 1,
    'provide_context': True,
}

dag = DAG(
    'fetch_and_produce_to_kafka',
    default_args=default_args,
    #schedule_interval=timedelta(minutes=120),
    schedule_interval="30 1,3,5,7,9,11,13,15,17,19,21,23 * * *",  # 홀수 시의 30분마다 실행
    catchup=False,
)

# Kafka 설정 메소드
def create_kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers=['172.31.9.242:19092','172.31.9.242:29092','172.31.9.242:39092'],
        key_serializer=None,
        value_serializer=lambda x: dumps(x).encode('utf-8'),
        api_version=(0, 11, 5)
    )
    return producer

# 교통 데이터 가져오고 Kafka로 전송하는 메소드
def fetch_and_produce_traffic_data(**kwargs):
    now = datetime.now()
    YMD = now.strftime('%Y%m%d')
    HH = now.strftime('%H')
    HH = int(HH) - 1
    if HH < 0:
        HH = 23
    if 0 <= HH <= 9:
        HH = '0' + str(HH)
    HH = str(HH)

    spot_list = ['A-01','A-02','A-03','A-04','A-05',
        'A-06','A-07','A-08','A-09','A-10',
        'A-11','A-12','A-13','A-14','A-15',
        'A-16','A-17','A-18','A-19','A-20',
        'A-21','A-22','A-23','A-24',
        'B-01','B-02','B-03','B-04','B-05',
        'B-06','B-07','B-08','B-09','B-10',
        'B-11','B-12','B-13','B-14','B-15',
        'B-16','B-17','B-18','B-19','B-20',
        'B-21','B-22','B-23','B-24','B-25',
        'B-26','B-27','B-28','B-29','B-30',
        'B-31','B-32','B-33','B-34','B-35',
        'B-36','B-37','B-38',
        'C-01','C-02','C-03','C-04','C-05',
        'C-06','C-07','C-08','C-09','C-10',
        'C-11','C-12','C-13','C-14','C-15',
        'C-16','C-17','C-18','C-19','C-20',
        'C-21',
        'D-01','D-02','D-03','D-04','D-05',
        'D-06','D-07','D-08','D-09','D-10',
        'D-11','D-12','D-13','D-14','D-15',
        'D-16','D-17','D-18','D-19','D-20',
        'D-21','D-22','D-23','D-24','D-25',
        'D-26','D-27','D-28','D-29','D-30',
        'D-31','D-32','D-33','D-34','D-35',
        'D-36','D-37','D-38','D-39','D-40',
        'D-41','D-42','D-43','D-44','D-45',
        'D-46',
        'F-01','F-02','F-03','F-04','F-05',
        'F-06','F-07','F-08','F-09','F-10']
    
    producer = create_kafka_producer()

    for spot in spot_list:
        url_traffic = f'http://openapi.seoul.go.kr:8088/414c516d6462656137354c486f7052/xml/VolInfo/1/5/{spot}/{YMD}/{HH}/'
        # print(url)
        response = requests.get(url_traffic)
        print(response.text)

        tree = elemTree.fromstring(response.text)
        print(len(tree.find('./row')))
        sum_vol = 0
        for row in tree.findall('./row'):
            print('---', spot, '---')
            vol = row.find('vol').text
            sum_vol += int(vol)
            print(sum_vol)

        #break
        data = {'date':YMD,
                 'hour':HH,
                 'spot':spot,
                 'vol':sum_vol}

        producer.send('kfk-traffic', value=data)
        producer.flush()

        
        
# 대기 오염 데이터 가져오고 Kafka로 전송하는 메소드
def fetch_and_produce_pollution_data(**kwargs):
    url_pollution = f'http://openAPI.seoul.go.kr:8088/414c516d6462656137354c486f7052/json/RealtimeCityAir/1/25/'

    response = requests.get(url_pollution)
    
    producer = create_kafka_producer()
    
    response_json = json.loads(response.text)
    for row in response_json["RealtimeCityAir"]["row"]:
        print(row["MSRDT"], row["MSRSTE_NM"], row["PM10"], row["PM25"], row["O3"],
              row["NO2"], row["CO"], row["SO2"], row["CO"])

        data = {'date':row["MSRDT"][:8],
                'hour':row["MSRDT"][8:10],
                'spot':row["MSRSTE_NM"],
                'PM10':row["PM10"], "PM25":row["PM25"], "O3":row["O3"], "NO2":row["NO2"],
                "CO":row["CO"], "SO2":row["SO2"]}
        print(data)
        
        topic = 'kfk-pollution'
        producer.send(topic, value=data)
        producer.flush()

# PythonOperator로 작업 정의
fetch_and_produce_traffic_task = PythonOperator(
    task_id='fetch_and_produce_traffic_data',
    provide_context=True,
    python_callable=fetch_and_produce_traffic_data,
    dag=dag,
)

fetch_and_produce_pollution_task = PythonOperator(
    task_id='fetch_and_produce_pollution_data',
    provide_context=True,
    python_callable=fetch_and_produce_pollution_data,
    dag=dag,
)

# 작업 간의 관계 설정
fetch_and_produce_traffic_task >> fetch_and_produce_pollution_task
