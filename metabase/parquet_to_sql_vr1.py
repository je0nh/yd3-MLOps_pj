from sqlalchemy import create_engine
import pyarrow.parquet as pq

# MySQL 연결 문자열 설정
engine = create_engine("mysql://team09:1234@localhost:1234/team09_database")

parquet_path = ['/home/ubuntu/metabase/data/pollution.parquet', '/home/ubuntu/metabase/data/traffic.parquet','/home/ubuntu/metabase/data/predict.parquet']

for idx, path in enumerate(parquet_path):
    # Parquet 파일 읽어오기
    parquet_table = pq.read_table(path)

    # 데이터프레임으로 변환
    df = parquet_table.to_pandas()

    if idx == 0:
        df.to_sql('pollution', engine, if_exists='replace', index=False)
    elif idx == 1:
        df.to_sql('traffic', engine, if_exists='replace', index=False)
    elif idx == 2:
        df.to_sql('predict', engine, if_exists='replace', index=False)
