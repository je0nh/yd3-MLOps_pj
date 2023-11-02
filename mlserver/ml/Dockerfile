# Dockerfile

FROM python:3.10.4
WORKDIR /app

# 필요한 파일들을 복사
COPY . /app
RUN python3 -m pip install --upgrade pip

RUN pip install jupyter

RUN pip install -r requirements.txt

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--allow-root"]
