from flask import Flask, jsonify
from joblib import load
import numpy as np
import pandas as pd
import requests
from hdfs import InsecureClient
app = Flask(__name__)

@app.route('/')
def hello_world():
    test_np_input = np.array([1,11,43.0, 16.0, 0.025, 0.005, 0.002, 0.2,2018,7,17]).reshape(1, -1).astype(float)
    model = load('C:/Users/jeong bok/Documents/cse101/team9PJ/mlops-team9/py/lgbm_model.pkl')
    preds = model.predict(test_np_input)
    print(preds)
    preds_as_str = str(preds)

    return preds_as_str
@app.route('/get_json_data', methods=['GET'])
def get_json_data():

    hdfs_url = 'http://43.201.229.213:50070'
    hdfs_path = '/test/seoul_pollution_2023-10-23-00_29.json'

    client = InsecureClient(hdfs_url, user='ubuntu')
    with client.read(hdfs_path) as reader:
        data = reader.read()

  # Return the data as JSON
    return jsonify({'data': data.decode('unicode_escape')})



if __name__ == '__main__':
    app.run(host='localhost',port=5000,debug=True)

