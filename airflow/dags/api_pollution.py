import requests
from datetime import datetime
import xml.etree.ElementTree as elemTree
from kafka import KafkaProducer
from json import dumps
import json
import fire

def load_pollution_data():

    producer = KafkaProducer(bootstrap_servers=[
        '172.31.9.242:19092', '172.31.9.242:29092', '172.31.9.242:39092'
    ],
                             key_serializer=None,
                             value_serializer=lambda x: dumps(x).encode('utf-8'),
                             api_version=(0, 11, 5))

    url_pollution = f'http://openAPI.seoul.go.kr:8088/414c516d6462656137354c486f7052/json/RealtimeCityAir/1/25/'

    response = requests.get(url_pollution)

    print(response.text, type(response.text))

    response_json = json.loads(response.text)
    for row in response_json["RealtimeCityAir"]["row"]:
        print(row["MSRDT"], row["MSRSTE_NM"], row["PM10"], row["PM25"], row["O3"],
              row["NO2"], row["CO"], row["SO2"], row["CO"])

        data = {
            'date': row["MSRDT"][:8],
            'hour': row["MSRDT"][8:10],
            'spot': row["MSRSTE_NM"],
            'PM10': row["PM10"],
            "PM25": row["PM25"],
            "O3": row["O3"],
            "NO2": row["NO2"],
            "CO": row["CO"],
            "SO2": row["SO2"],
            "CO": row["CO"]
        }
        print(data)

        producer.send('kfk-pollution', value=data)
        producer.flush()
        
if __name__ == "__main__":
    fire.Fire(load_pollution_data)
