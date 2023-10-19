import requests
import xml.etree.ElementTree as elemTree
from kafka import KafkaProducer
import json

class PollutionProcessor:
    
    def __init__(self):
        super().__init__()
        #self.param1 = param1
        #self.param2 = param2

    def load_pollution_data(self):
        datas = []

        url_pollution = f'http://openAPI.seoul.go.kr:8088/414c516d6462656137354c486f7052/json/RealtimeCityAir/1/25/'

        response = requests.get(url_pollution)

        #print(response.text, type(response.text))

        response_json = json.loads(response.text)
        for row in response_json["RealtimeCityAir"]["row"]:
            #print(row["MSRDT"], row["MSRSTE_NM"], row["PM10"], row["PM25"], row["O3"], row["NO2"], row["CO"], row["SO2"], row["CO"])

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
            #print(data)
            datas.append(data)

        return json.dumps(datas, ensure_ascii=False)
