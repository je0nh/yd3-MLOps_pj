from joblib import load
import joblib
import numpy as np
import pandas as pd
import requests

from hdfs import InsecureClient
import json
from pyhive import hive
import happybase as hb
c = hb.Connection('172.31.15.100', 9090, autoconnect=False)
c.open()
print(c.tables())

conn = hive.Connection(host='172.31.1.129', port=10000,
                       username='hive', password='1234', auth='LDAP')
cursor = conn.cursor()

query = '''
select
pollution.ymd as ymd,
pollution.hour as hour,
pollution.spot as spot,
pollution.pm10 as pm10,
pollution.pm25 as pm25,
pollution.o3 as o3,
pollution.no2 as no2,
pollution.co as co,
pollution.so2 as so2
from pollution
'''
# 저장되는 데이터칼럼을 수정해서 현재시간의 데이터만 불러오도록 해야함 예시코드
# WHERE ymd = '20231025' AND hour >= '1200'
#   AND your_timestamp_column >= current_timestamp;
query2 = '''
select traffic.ymd as ymd,
traffic.hour as hour ,
traffic.spot as spot,
traffic.vol as vol
from traffic
'''

# 쿼리 실행
cursor.execute(query)

# 컬럼 정보 가져오기
columns = [desc[0] for desc in cursor.description]

# 결과 가져오기
data = cursor.fetchall()
cursor.execute(query2)
columns1 = [desc[0] for desc in cursor.description]
data1 = cursor.fetchall()

# 커서와 연결 닫기
conn.close()

# 결과를 데이터프레임으로 변환
air_df = pd.DataFrame(data, columns=columns)
tr_df = pd.DataFrame(data1, columns=columns1)
# 결과 출력
air_df['ymd'] = pd.to_datetime(air_df['ymd'], format='%Y%m%d')
col= { 'spot': 'gu','hour':'time'}
air_df.rename(columns=col,inplace=True)
air_df = air_df[air_df['gu'] !='강북구']
air_df['year']=air_df['ymd'].dt.year
air_df['month']=air_df['ymd'].dt.month
air_df['day'] =air_df['ymd'].dt.day
air_df = air_df.drop(columns='ymd')
# 파일에서 객체 로드
label_encoder = joblib.load('label_encoder_classes.joblib')

# 로드된 객체를 사용하여 변환
air_df['gu'] = label_encoder.transform(air_df['gu'])

# 또는 반대로 변환도 가능
# decoded_labels = label_encoder.inverse_transform(encoded_labels)

air_df = air_df.astype(float)


air_df['time']=air_df['time'].astype(int)
air_df['gu']=air_df['gu'].astype(int)
air_df['month']=air_df['month'].astype(int)
air_df['year']=air_df['year'].astype(int)
air_df['day']=air_df['day'].astype(int)
air_df['gu']= air_df['gu'].astype(int)


model = load('./lgbm_model.pkl')
preds = model.predict(air_df)

air_df['traffic']=preds
air_df['gu'] = label_encoder.inverse_transform(air_df['gu'])
gu_df = pd.read_csv('./spot_data_guz.csv')
cols ={ 'spot_num' : 'spot' }
gu_df.rename(columns=cols,inplace=True)

tr_df = pd.merge(gu_df, tr_df, on=['spot'])
tr_df.drop(columns='spot',inplace=True)

tr_df['vol']=tr_df['vol'].astype(int)
tr_df['gu']=tr_df['gu'].astype(object)
tr_df['hour']=tr_df['hour'].astype(int)
tr_df['ymd']=tr_df['ymd'].astype(int)


tr_df = tr_df.groupby(['gu','ymd','hour'])[['vol']].sum().reset_index()

new_df=pd.merge(tr_df,air_df, on=['gu'])
drop_cols = { 'time','year','month','day'}
new_df.drop(columns=drop_cols,inplace=True)
new_df['ymd']= pd.to_datetime(new_df['ymd'],format='%Y%m%d')
new_df['hour'] = new_df['hour'].astype(int)
use_col = ['gu','ymd','hour','traffic']
l_df = new_df[use_col]
# Hive 연결
conn = hive.Connection(host='172.31.1.129', port=10000,
                       username='hive', password='1234', auth='LDAP')
cursor = conn.cursor()

# 예측한 데이터프레임을 Hive에 저장하는 쿼리 작성
for index, row in l_df.piterrows():
    insert_query = f'''
    INSERT INTO TABLE default.predict 
    VALUES ('{row['gu']}', '{row['ymd']}', {row['hour']}, {row['traffic']})
    '''
    cursor.execute(insert_query)

# 변경 사항 커밋
conn.commit()

# 연결 종료
conn.close()





