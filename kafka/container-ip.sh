#!/bin/bash

# Kafka 컨테이너의 ID나 이름을 변수로 저장
kafka1="kafka1"
kafka2="kafka2"
kafka3="kafka3"

# Kafka 컨테이너의 IP 주소를 추출하여 각각의 환경 변수에 할당
kafka1_address=$(docker container inspect $kafka1 | grep -oP '"IPAddress": "\K[^"]+')
kafka2_address=$(docker container inspect $kafka2 | grep -oP '"IPAddress": "\K[^"]+')
kafka3_address=$(docker container inspect $kafka3 | grep -oP '"IPAddress": "\K[^"]+')

# 각 Kafka 서버의 IP 주소를 환경 변수로 설정
export KAFKA1_SERVER=$kafka1_address
export KAFKA2_SERVER=$kafka2_address
export KAFKA3_SERVER=$kafka3_address

# 각 서버의 주소를 출력
echo "KAFKA1_SERVER is set to $KAFKA1_SERVER"
echo "KAFKA2_SERVER is set to $KAFKA2_SERVER"
echo "KAFKA3_SERVER is set to $KAFKA3_SERVER"
