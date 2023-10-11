import subprocess
from kafka import KafkaProducer
from json import dumps
import time

shell_command = 'bash ./container-ip.sh'

result = subprocess.run(shell_command, shell=True, check=True, stdout=subprocess.PIPE, text=True)

# 결과를 공백을 기준으로 분할하여 IP 주소를 추출합니다.
ip_addresses = result.stdout.strip().split()

# 추출된 IP 주소 출력
kafka1_server = ip_addresses[4] + ':19094'
kafka2_server = ip_addresses[9] + ':29094'
kafka3_server = ip_addresses[14] + ':39094'

def on_send_success(record_metadata):
    # 보낸데이터의 매타데이터를 출력한다
    print("record_metadata:", record_metadata)
    
# 카프카 서버
bootstrap_servers = [kafka1_server, kafka2_server, kafka3_server]

# 카프카 공급자 생성
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         key_serializer=None,
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

# 카프카 토픽
str_topic_name = 'kfk-test'

# 카프카 공급자 토픽에 데이터를 보낸다
data = {"time": time.time()}
producer.send(str_topic_name, value=data).add_callback(on_send_success)\
                                         .get(timeout=100) # blocking maximum timeout
print('data:', data)
