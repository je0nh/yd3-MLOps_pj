from pyhive import hive
import pandas as pd
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq

# 날짜 형식 변환 함수
def convert_datetime(data):
    date_objects = [datetime.strptime(date, '%Y%m%d') for date in data]
    formatted_dates = [date.strftime('%Y-%m-%d') for date in date_objects]
    return formatted_dates

# Hive 서버 연결 정보
host = '43.200.18.14'
port = 10000
conn = hive.Connection(host=host, port=port)
cur = conn.cursor()

# Hive에서 데이터를 가져오는 쿼리들
get_data_querys = [
    "SELECT * FROM pollution",
    "SELECT * FROM traffic",
    "SELECT * FROM predict"
]

# 쿼리 실행 및 데이터 처리 반복
for idx, get_data_query in enumerate(get_data_querys):
    cur.execute(get_data_query)
    data = cur.fetchall()
    n_data = pd.DataFrame(data)

    if idx == 0:
        # 오염 데이터 처리
        n_data.columns = ['ymd', 'time', 'spot', 'pm10', 'pm25', 'o3', 'no2', 'co', 'so2']
        n_data['ymd'] = convert_datetime(n_data['ymd'])
        print(n_data)
        n_data.to_parquet('pollution.parquet', index=False)

    elif idx == 1:
        # 교통 데이터 처리
        df = pd.read_table('./cov_gu.txt', sep=",")
        spot_num = dict(zip(df['spot_num'], df['gu']))
        n_data['spot'] = n_data[2].map(spot_num)
        n_data = n_data.rename(columns={0:'ymd', 1:'time', 3:'traffic_vol'})
        n_data = n_data.drop(2, axis=1)
        n_data = n_data[['ymd', 'time', 'spot', 'traffic_vol']]
        n_data['ymd'] = convert_datetime(n_data['ymd'])

    elif idx == 2:
        # 예측 데이터 처리
        n_data = n_data.rename(columns={0:'spot', 1:'ymd', 2:'time', 3:'pred_traffic_vol'})
        n_data = n_data[['ymd', 'time', 'spot', 'pred_traffic_vol']]
        print(n_data)
        n_data.to_parquet('predict.parquet', index=False)
