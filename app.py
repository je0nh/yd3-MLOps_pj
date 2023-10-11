from flask import Flask, render_template, request, jsonify
from flask_mysqldb import MySQL
import MySQLdb
from flask_wtf import FlaskForm
from wtforms import SelectField, StringField, SubmitField
import pandas as pd
import lightgbm as lgb
import joblib

# Flask 어플리케이션 생성
app = Flask(__name__)
app.secret_key = 'abc'
# 모델 로드
model = joblib.load('trained_model.joblib')

# Sample Form
class MyForm(FlaskForm):
    # Add any form fields you need here
    submit = SubmitField('Submit')

@app.route('/predict', methods=['POST', 'GET'])
def predict():
    predictions = None
    if request.method == 'POST':
        # POST 요청으로 데이터를 받아옴
        data = request.get_json(force=True)

        # JSON 데이터를 DataFrame으로 변환
        input_data = pd.DataFrame(data)

        # 모델을 사용하여 예측
        predictions = model.predict(input_data)

        # 예측 결과를 JSON 형태로 반환
        return jsonify(predictions.tolist())

    elif request.method == 'GET':
        # If it's a GET request, just render the form
        form = MyForm()
        return render_template('prediction_form.html', form=form, predictions=predictions)


if __name__ == '__main__':
    app.run(port=5000, debug=True)

