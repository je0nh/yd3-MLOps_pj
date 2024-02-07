## 이어드림 3기 - MLOps 파이프라인 구현 프로젝트 
- 프로젝트 기간: 23.09.26 ~ 23.11.03 (7주)
- 개발인원: 5명 (김경석, 김준호, 이정복, 이정훈, 전승욱)

  |이름|역할|
  |---|---|
  |김경석, 전승욱| 데이터 레이크 구축|
  |김준호|ML|
  |이정복|데이터 수집 및 전처리|
  |이정훈|데이터 수집 및 전처리, 데이터 시각화|

## 프로젝트 목표
<p align="center">
  <img width="620" alt="Screenshot 2024-01-16 at 6 27 43 PM" src="https://github.com/je0nh/yd3-MLOps_pj/assets/145730125/069a144a-04fe-4039-9666-94ba4eb088b2">
<p/>

- 서울 열린데이터광장에서 교통량 데이터와 대기 데이터를 받아와 대기 오염정도에 따른 교통량을 예측하는 파이프라인 구축
- 데이터 수집부터 시각화까지 데이터 파이프라인의 end-to-end 구현
- 자동화 파이프라인 구축

## Architecture
<p align="center">
  <img width="620" alt="Screenshot 2024-01-08 at 12 36 36 PM" src="https://github.com/je0nh/yd3-MLOps_pj/assets/145730125/4d7de6ec-e630-4b48-b799-43adfd49defa">
<p/>

## 프로젝트 진행과정
1. 여러곳에서 실시간으로 데이터를 가져오기 용이한 Kafka를 이용해서 데이터 파이프라인 구축
2. Hadoop을 이용한 데이터레이크 구축
3. 데이터레이크에서 데이터를 가져와 Light GBM을 통해 교통량 예측
4. Flask API를 통해 외부에서 예측치를 호출 가능하도록 함
5. Metabase를 통해 시각화

## Stack
**Environment** <br>
<img src="https://img.shields.io/badge/jupyter-F37626?style=for-the-badge&logo=jupyter&logoColor=white">
<img src="https://img.shields.io/badge/git-F05032?style=for-the-badge&logo=git&logoColor=white">
<img src="https://img.shields.io/badge/github-181717?style=for-the-badge&logo=github&logoColor=white">

**Language** <br>
<img src="https://img.shields.io/badge/python-3776AB?style=for-the-badge&logo=python&logoColor=white">
<img src="https://img.shields.io/badge/scala-DC322F?style=for-the-badge&logo=scala&logoColor=white">

**Config** <br>
<img src="https://img.shields.io/badge/amazonec2-FF9900?style=for-the-badge&logo=amazonec2&logoColor=white">
<img src="https://img.shields.io/badge/ubuntu-E95420?style=for-the-badge&logo=ubuntu&logoColor=white">
<img src="https://img.shields.io/badge/docker-2496ED?style=for-the-badge&logo=docker&logoColor=white">

**Framework** <br>
<img src="https://img.shields.io/badge/apacheairflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white">
<img src="https://img.shields.io/badge/apachekafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white">
<img src="https://img.shields.io/badge/apachespark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white">
<img src="https://img.shields.io/badge/apachehadoop-66CCFF?style=for-the-badge&logo=apachehadoop&logoColor=white">
<img src="https://img.shields.io/badge/flask-000000?style=for-the-badge&logo=flask&logoColor=white">
<img src="https://img.shields.io/badge/mysql-4479A1?style=for-the-badge&logo=mysql&logoColor=white">
<img src="https://img.shields.io/badge/metabase-509EE3?style=for-the-badge&logo=metabase&logoColor=white">

**Communication** <br>
<img src="https://img.shields.io/badge/notion-000000?style=for-the-badge&logo=notion&logoColor=white">
<img src="https://img.shields.io/badge/slack-4A154B?style=for-the-badge&logo=slack&logoColor=white">

## 프로젝트 구현 내용
1. Kafka를 통해 데이터 수신
<p align="center">
  <img width="620" alt="Screenshot 2024-01-16 at 6 45 43 PM" src="https://github.com/je0nh/yd3-MLOps_pj/assets/145730125/3c747c01-4b1b-4647-a089-9e5539520a7c">
</p>

2. Spark를 통한 map, reduce

3. HDFS에 데이터 적제
<p align="center"> 
  <img width="620" alt="Screenshot 2024-01-16 at 6 47 06 PM" src="https://github.com/je0nh/yd3-MLOps_pj/assets/145730125/4e7dc8d5-9d9a-40b8-b9a2-088fdfb6e98b">
</p>

4. Flask API 호출
<p align="center"> 
  <img width="620" alt="Screenshot 2024-01-16 at 6 49 37 PM" src="https://github.com/je0nh/yd3-MLOps_pj/assets/145730125/d3539ebc-eca9-402c-aa09-a3bc03631950">
</p>

5. Metabase를 이용한 시각화
<p align="center">
  <img width="620" alt="Screenshot 2024-01-16 at 6 50 29 PM" src="https://github.com/je0nh/yd3-MLOps_pj/assets/145730125/042ec8ea-1c3f-4d8b-9194-9c6bb6f03824">
</p>

## 프로젝트 한계 및 개선방안
- 데이터 특성에 관해 이해를 잘못해서 파이프라인 앞단의 부분이 이상하게 되어 있음 -> 데이터 특성을 고려한 파이프라인 재설계가 필요함
- 시간상의 한계로 HBASE는 사용하지 못함

## 시연 & 노션페이지 (클릭시 페이지 이동)
[<p align="center"><img width="620" alt="Screenshot 2024-02-07 at 4 41 46 PM" src="https://github.com/je0nh/yd3-MLOps_pj/assets/145730125/6a67972d-7887-4753-ae1f-ce13705ede3a"></p>](https://youtu.be/3kgrXjSlzMA)
[<p align="center"><img width="620" alt="Screenshot 2024-02-07 at 4 42 42 PM" src="https://github.com/je0nh/yd3-MLOps_pj/assets/145730125/20bba379-45d3-44b3-b107-ab1c4d228b49"></p>](https://round-helicopter-e2f.notion.site/c52684bff0dc45d4b0878d9c33ff4d3a?v=e88f973a87a94685af244e6cf56ff8d9)
[<p align="center"><img width="620" alt="Screenshot 2024-02-07 at 4 43 14 PM" src="https://github.com/je0nh/yd3-MLOps_pj/assets/145730125/6d854d6b-d20d-4565-b3c2-da9f5d21a8ab"></p>](https://round-helicopter-e2f.notion.site/772374157b3d4ca6ae459ec05e245009?v=8d3f7bb665074f4d8e51005d58db3ce3)

## 발표 ppt
[<p align="center"><img width="620" alt="Screenshot 2024-02-07 at 4 40 50 PM" src="https://github.com/je0nh/yd3-MLOps_pj/assets/145730125/a0dcdd2f-c088-474c-bdf7-bf07889a0fd8"></p>](https://github.com/je0nh/yd3-MLOps_pj/files/13856226/9.pdf)




