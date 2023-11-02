from flask import Flask, jsonify
from joblib import load
import numpy as np
import pandas as pd
import requests
from hdfs import InsecureClient
from pyhive import hive
import time
app = Flask(__name__)

@app.route('/')
def write_to_hive(df, table_name):
    # Hive 연결 정보
    host = '172.31.9.242'
    port = 10000
    username = 'hive'
    password = '1234'

    # Hive 연결
    conn = hive.Connection(host=host, port=port, username=username, password=password)

    # 데이터프레임을 Hive 테이블로 로드
    df.to_sql(table_name, con=conn, if_exists='replace', index=False, chunksize=1000)

    # 연결 닫기
    conn.close()
    return preds_as_str
@app.route('/get_json_data', methods=['GET'])
def get_json_data():

    hdfs_url = 'http://43.201.229.213:50070'
    hdfs_path = '/test/seoul_pollution_2023-10-23-00_29.json'

    client = InsecureClient(hdfs_url, user='ubuntu')
    with client.read(hdfs_path) as reader:
        data = reader.read()
    data_json = json.loads(data.decode('unicode_escape'))
    df = pd.DataFrame(data_json)
    df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
    col= { 'spot': 'gu','hour':'time'}
    df.rename(columns=col,inplace=True)
    df = df[df['gu'] !='강북구']
    df['year']=df['date'].dt.year
    df['month']=df['date'].dt.month
    df['day'] =df['date'].dt.day
    df = df.drop(columns='date')
    

    # 파일에서 객체 로드
    label_encoder = joblib.load('label_encoder_classes.joblib')

    # 로드된 객체를 사용하여 변환
    df['gu'] = label_encoder.transform(df['gu'])

    # 또는 반대로 변환도 가능
    # decoded_labels = label_encoder.inverse_transform(encoded_labels)
    df['time'] =df['time'].astype(int)
    model = load('/app/myapp/lgbm_model.pkl')
    preds = model.predict(df)
    df['traffic'] = preds
    df['gu'] = label_encoder.inverse_transform(df['gu'])
    hdfs_paths = '/test/seoul_traffic_2023-10-23-00_29.json'
    # HDFS 클라이언트 생성
    client = InsecureClient(hdfs_url, user='ubuntu')

    # HDFS에서 데이터 읽기
    with client.read(hdfs_paths) as reader:
        data = reader.read()

    data_json = json.loads(data.decode('unicode_escape'))
    tr_df = pd.DataFrame(data_json)
    gu_df = pd.read_csv('/app/myapp/spot_data_guz.csv')
    cols ={ 'spot_num' : 'spot' }
    gu_df.rename(columns=cols,inplace=True)
    tr_df = pd.merge(gu_df, tr_df, on=['spot'])
    tr_df.drop(columns='spot',inplace=True)
    tr_df = tr_df.groupby(['gu','date','hour'])[['vol']].sum().reset_index()
    new_df=pd.merge(tr_df,df, on=['gu'])
    drop_cols = { 'time','year','month','day'}
    new_df.drop(columns=drop_cols,inplace=True)
    new_df['date']= pd.to_datetime(new_df['date'],format='%Y%m%d')
    new_df['hour'] = new_df['hour'].astype(int)
    
  # Return the data as JSON
    return new_df



if __name__ == '__main__':
    app.run(host='0.0.0.0',port=5002)
