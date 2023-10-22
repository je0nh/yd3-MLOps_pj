import subprocess
from kafka import KafkaConsumer
from json import loads

# 추출된 IP 주소 출력
kafka1_server = '13.124.248.100:19094'
kafka2_server = '13.124.248.100:29094'
kafka3_server = '13.124.248.100:39094'

# 카프카 서버
bootstrap_servers = [kafka1_server, kafka2_server, kafka3_server]

# 카프카 토픽
str_topic_name = 'kfk-pollution'

# 카프카 소비자 group1 생성
str_group_name = 'group1'
consumer = KafkaConsumer(str_topic_name, bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='earliest', # 가장 처음부터 소비
                         enable_auto_commit=True,
                         group_id=str_group_name,
                         value_deserializer=lambda x: loads(x.decode('utf-8')),
                         consumer_timeout_ms=60000 # 타임아웃지정(단위:밀리초)
                        )

for message in consumer:
    message_data = message.value
    print(message_data)
