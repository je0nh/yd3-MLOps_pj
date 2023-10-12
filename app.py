from flask import Flask
from joblib import load
import numpy as np
import pandas as pd
app = Flask(__name__)

@app.route('/')
def hello_world():
    test_np_input = np.array([1,21,43.0, 16.0, 0.025, 0.005, 0.012, 0.2,2018,4,17]).reshape(1, -1).astype(float)
    model = load('lgbm_model.pkl')
    preds = model.predict(test_np_input)
    print(preds)
    preds_as_str = str(preds)
    return preds_as_str

if __name__ == '__main__':
    app.run()
