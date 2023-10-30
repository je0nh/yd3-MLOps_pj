from sqlalchemy import create_engine, Column, Integer, String, Date
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import pyarrow.parquet as pq

# MySQL 연결 문자열 설정
engine = create_engine("mysql://team09:1234@localhost:1234/team09_database")

# 데이터베이스와 세션 생성
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()

# 데이터 모델 정의
class Pollution(Base):
    __tablename__ = 'pollution'
    ymd = Column(Date, primary_key=True)
    time = Column(Integer)
    spot = Column(String(255))  # 이 부분에서 VARCHAR의 최대 길이를 지정하세요
    pm10 = Column(Integer)
    pm25 = Column(Integer)
    o3 = Column(Integer)
    no2 = Column(Integer)
    co = Column(Integer)
    so2 = Column(Integer)

class Traffic(Base):
    __tablename__ = 'traffic'
    ymd = Column(Date, primary_key=True)
    time = Column(Integer)
    spot = Column(String(255))  # 이 부분에서 VARCHAR의 최대 길이를 지정하세요
    traffic_vol = Column(Integer)

class Predict(Base):
    __tablename__ = 'predict'
    ymd = Column(Date, primary_key=True)
    time = Column(Integer)
    spot = Column(String(255))  # 이 부분에서 VARCHAR의 최대 길이를 지정하세요
    pred_traffic_vol = Column(Integer)

# Parquet 파일 경로
parquet_path = ['/home/ubuntu/metabase/data/pollution.parquet', '/home/ubuntu/metabase/data/traffic.parquet','/home/ubuntu/metabase/data/predict.parquet']

for idx, path in enumerate(parquet_path):
    # Parquet 파일 읽어오기
    parquet_table = pq.read_table(path)

    # 데이터프레임으로 변환
    df = parquet_table.to_pandas()

    if idx == 0:
        # 데이터프레임을 데이터베이스 테이블로 저장
        df.to_sql('pollution', engine, if_exists='replace', index=False, dtype={
            'ymd': Date(),
            'time': Integer(),
            'spot': String(255),
            'pm10': Integer(),
            'pm25': Integer(),
            'o3': Integer(),
            'no2': Integer(),
            'co': Integer(),
            'so2': Integer()
        })
    elif idx == 1:
        df.to_sql('traffic', engine, if_exists='replace', index=False, dtype={
            'ymd': Date(),
            'time': Integer(),
            'spot': String(255),
            'traffic_vol': Integer()
        })
    elif idx == 2:
        df.to_sql('predict', engine, if_exists='replace', index=False, dtype={
            'ymd': Date(),
            'time': Integer(),
            'spot': String(255),
            'pred_traffic_vol': Integer()
        })

# 세션 종료
session.close()
