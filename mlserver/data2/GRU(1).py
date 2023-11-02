import numpy as np
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import GRU, Dense
import time
from pyhive import hive

# 초기 학습을 위한 가상의 데이터 생성
# ...
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

# 예측 결과 데이터프레임 생성 (가상의 데이터로 가정)
prediction_data = pd.DataFrame({
    'timestamp': pd.date_range(start='2023-01-01', periods=10, freq='D'),
    'predicted_value': np.random.rand(10)
})

# 주기적으로 Hive에 데이터 전송
update_cycle = 1
while True:
    # 데이터프레임을 Hive에 전송
    write_to_hive(prediction_data, table_name='your_hive_table')

    print(f"Completed update cycle {update_cycle}")

    # 2시간 대기
    time.sleep(2 * 60 * 60)  # 2시간을 초로 변환하여 대기

    update_cycle += 1
# 초기 모델 생성
model = Sequential()
model.add(GRU(units=50, input_shape=(X_train.shape[1], X_train.shape[2])))
model.add(Dense(units=1))
model.compile(optimizer='adam', loss='mean_squared_error')

# 초기 학습
model.fit(X_train, y_train, epochs=10)

# 주기적인 업데이트 함수 정의
def periodic_update(new_data_X, new_data_y, model):
    # 새로운 데이터로 모델 업데이트
    model.fit(new_data_X, new_data_y, epochs=5)
    return model

# 2시간마다 주기적 업데이트 수행
update_cycle = 1
while True:
    # 새로운 데이터 생성 (편의상 가상의 데이터로 가정)
    new_data_X = np.random.randn(10, X_train.shape[1], X_train.shape[2])
    new_data_y = np.random.randn(10, 1)

    # 주기적인 업데이트 수행
    model = periodic_update(new_data_X, new_data_y, model)

    print(f"Completed update cycle {update_cycle}")

    # 2시간 대기
    time.sleep(2 * 60 * 60)  # 2시간을 초로 변환하여 대기

    update_cycle += 1
