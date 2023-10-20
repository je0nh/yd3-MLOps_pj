import subprocess
from kafka import KafkaProducer
from json import dumps
import time
import requests
import json

class Producer:
    
    def __init__(self):
        super().__init__()
        
    def on_send_success(self,record_metadata):
            # 보낸데이터의 매타데이터를 출력한다
            print("record_metadata:", record_metadata)
       
    def send(self,topic,data):
        
        # shell_command = 'bash ./container-ip.sh'

        # result = subprocess.run(shell_command, shell=True, check=True, stdout=subprocess.PIPE, text=True)

        # 결과를 공백을 기준으로 분할하여 IP 주소를 추출합니다.
        # ip_addresses = result.stdout.strip().split()

        # 추출된 IP 주소 출력
        # kafka1_server = ip_addresses[4] + ':19094'
        # kafka2_server = ip_addresses[9] + ':29094'
        # kafka3_server = ip_addresses[14] + ':39094'


        kafka1_server = '172.31.9.242:19092'
        kafka2_server = '172.31.9.242:29092'
        kafka3_server = '172.31.9.242:39092'

        # 카프카 서버
        bootstrap_servers = [kafka1_server, kafka2_server, kafka3_server]
        
        # 카프카 공급자 생성
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=None,
            value_serializer=lambda x: dumps(x).encode('utf-8'),
            api_version=(0, 11, 5)
        )
        
        kor = json.dumps(data, ensure_ascii=False)

        # 카프카 토픽    
        producer.send(topic, value=data).add_callback(self.on_send_success).get(timeout=100)# blocking maximum timeout
        producer.flush()
        
        print(f"topic {topic} , data : {kor}")
