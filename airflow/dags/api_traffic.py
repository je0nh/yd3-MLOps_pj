import requests
from datetime import datetime
import xml.etree.ElementTree as elemTree
from kafka import KafkaProducer
from json import dumps
import json
import fire

def load_traffic_data():
    
    producer = KafkaProducer(bootstrap_servers=[
        '172.31.9.242:19092', '172.31.9.242:29092', '172.31.9.242:39092'
    ],
                             key_serializer=None,
                             value_serializer=lambda x: dumps(x).encode('utf-8'),
                             api_version=(0, 11, 5))

    now = datetime.now()
    YMD = now.strftime('%Y%m%d')
    HH = now.strftime('%H')
    HH = int(HH) - 1
    if HH < 0:
        HH = 23
    if 0 <= HH <= 9:
        HH = '0' + str(HH)
    HH = str(HH)
    print(HH)
    spot_list = [
        'A-01', 'A-02', 'A-03', 'A-04', 'A-05', 'A-06', 'A-07', 'A-08', 'A-09',
        'A-10', 'A-11', 'A-12', 'A-13', 'A-14', 'A-15', 'A-16', 'A-17', 'A-18',
        'A-19', 'A-20', 'A-21', 'A-22', 'A-23', 'A-24', 'B-01', 'B-02', 'B-03',
        'B-04', 'B-05', 'B-06', 'B-07', 'B-08', 'B-09', 'B-10', 'B-11', 'B-12',
        'B-13', 'B-14', 'B-15', 'B-16', 'B-17', 'B-18', 'B-19', 'B-20', 'B-21',
        'B-22', 'B-23', 'B-24', 'B-25', 'B-26', 'B-27', 'B-28', 'B-29', 'B-30',
        'B-31', 'B-32', 'B-33', 'B-34', 'B-35', 'B-36', 'B-37', 'B-38', 'C-01',
        'C-02', 'C-03', 'C-04', 'C-05', 'C-06', 'C-07', 'C-08', 'C-09', 'C-10',
        'C-11', 'C-12', 'C-13', 'C-14', 'C-15', 'C-16', 'C-17', 'C-18', 'C-19',
        'C-20', 'C-21', 'D-01', 'D-02', 'D-03', 'D-04', 'D-05', 'D-06', 'D-07',
        'D-08', 'D-09', 'D-10', 'D-11', 'D-12', 'D-13', 'D-14', 'D-15', 'D-16',
        'D-17', 'D-18', 'D-19', 'D-20', 'D-21', 'D-22', 'D-23', 'D-24', 'D-25',
        'D-26', 'D-27', 'D-28', 'D-29', 'D-30', 'D-31', 'D-32', 'D-33', 'D-34',
        'D-35', 'D-36', 'D-37', 'D-38', 'D-39', 'D-40', 'D-41', 'D-42', 'D-43',
        'D-44', 'D-45', 'D-46', 'F-01', 'F-02', 'F-03', 'F-04', 'F-05', 'F-06',
        'F-07', 'F-08', 'F-09', 'F-10'
    ]

    for spot in spot_list:
        url_traffic = f'http://openapi.seoul.go.kr:8088/414c516d6462656137354c486f7052/xml/VolInfo/1/5/{spot}/{YMD}/{HH}/'
        # print(url)
        response = requests.get(url_traffic)
        print(response.text)

        tree = elemTree.fromstring(response.text)
        #print(len(tree.find('./row')))
        sum_vol = 0
        for row in tree.findall('./row'):
            print('---', spot, '---')
            vol = row.find('vol').text
            sum_vol += int(vol)
            print(sum_vol)

        data = {'date': YMD, 'hour': HH, 'spot': spot, 'vol': sum_vol}

        producer.send('kfk-traffic', value=data)
        producer.flush()

if __name__ == "__main__":
    fire.Fire(load_traffic_data)
