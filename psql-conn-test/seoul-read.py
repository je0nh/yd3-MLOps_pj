import csv
from sqlalchemy import create_engine

df = pd.read_csv('./new_AIR_HOUR_2021.csv')
url = "postgresql://team09:1234@43.200.18.14:5432/test"
con = create_engine(url, echo=False)
df.to_sql('test', con, if_exists='append', index=False)