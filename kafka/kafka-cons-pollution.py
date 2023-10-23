import subprocess
from kafka import KafkaConsumer
from json import loads
import json
from hdfs import InsecureClient
import requests
import re
from datetime import datetime

# 현재 시간 가져오기
current_time = datetime.now()

# 연도, 월, 일, 시, 분 정보를 문자열로 변환
formatted_time = current_time.strftime('%Y-%m-%d-%H')

# 특수 문자 및 공백을 밑줄(_)로 대체
cleaned_time = re.sub(r'[^\w\s-]', '_', formatted_time)

# Kafka 서버 설정
kafka_servers = ['13.124.248.100:19094', '13.124.248.100:29094', '13.124.248.100:39094']
topic_name = 'kfk-pollution'
group_name = 'group1'

# Kafka 소비자 설정
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=kafka_servers,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=group_name,
    value_deserializer=lambda x: loads(x.decode('utf-8')),
    consumer_timeout_ms=60000
)

for message in consumer:
    message_data = message.value
    print(message_data)

    # HDFS 클라이언트 설정
    hdfs_client = InsecureClient('http://43.201.229.213:50070/', user='ubuntu')

    # JSON 데이터
    json_data = message_data

    # JSON 데이터를 문자열로 변환
    json_str = json.dumps(json_data)

    # HDFS 경로 및 파일명 설정
    hdfs_path = f'/test/seoul_pollution_{cleaned_time}.json'

    # 기존 파일 삭제
    if hdfs_client.content(hdfs_path, strict=False):
        hdfs_client.delete(hdfs_path)

    # HDFS에 파일 쓰기
    with hdfs_client.write(hdfs_path) as writer:
        writer.write(json_str.encode('utf-8'))

    print("JSON 파일이 HDFS에 저장되었습니다. 경로:", hdfs_path)


