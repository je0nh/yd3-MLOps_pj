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
from flask import Flask, jsonify , render_template ,request
from joblib import load
import joblib
import numpy as np
import pandas as pd
import requests
from hdfs import InsecureClient
import json
from pyhive import hive
app = Flask(__name__)

@app.route('/')

def seoul_map():
    return render_template('seoul_map.html')

@app.route('/get_traffic_data')
def get_traffic_data():
    # Hive 연결
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

           # DataFrame을 HTML 표로 변환
#    html_data = new_df.to_html()
     # HTML 형식으로 반환
#    return render_template('data_template.html', table=html_data)
    result_json = new_df.to_json(orient='records')

    return result_json




# 모델 저장
model_path = 'lgbm_model.pkl'
# joblib.dump(model, model_path)

# # 주기적인 업데이트 함수 정의
# def periodic_update(new_data_X, new_data_y, model_path):
#     # 모델 로드
#     model = joblib.load('/app/myapp/lgbm_model.pkl')

#     # 새로운 데이터로 모델 업데이트
#     model.fit(new_data_X, new_data_y)

#     # 모델 저장
#     joblib.dump(model, model_path)

#     return model

# 예측 함수
def predict_from_model(data, model_path):
    # 모델 로드
    model = joblib.load('/app/myapp/lgbm_model.pkl')

    # 전처리 등이 필요하다면 여기에 추가
    # ...

    # 모델에 입력 데이터를 전달하여 예측
    prediction = model.predict(data)
    return prediction

# /predict 엔드포인트
@app.route('/predict', methods=['POST'])
def predict():
    try:
        # POST 요청에서 데이터 추출
        data = request.json.get('data')

        # 데이터를 NumPy 배열로 변환
        data = np.array(data)

        # 모델에 예측 요청
        prediction = predict_from_model(data, model_path)

        # 예측 결과를 JSON 형식으로 반환
        return jsonify({'prediction': prediction.tolist()})

    except Exception as e:
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    app.run(host='0.0.0.0',port=5002)
