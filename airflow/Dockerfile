FROM apache/airflow:2.7.1
COPY requirements.txt .
RUN pip install -r requirements.txt
ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Asia/Seoul
