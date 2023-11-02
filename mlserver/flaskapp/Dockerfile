FROM python:3.11.4
WORKDIR /app
COPY . /app

RUN python3 -m pip install --upgrade pip



RUN pip install -r requirements.txt
WORKDIR ./myapp
CMD gunicorn app:app --bind 0.0.0.0:5002
EXPOSE 5002

