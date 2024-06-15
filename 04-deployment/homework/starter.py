import pickle
import pandas as pd
import numpy as np

with open('model.bin', 'rb') as f_in:
    dv, model = pickle.load(f_in)


categorical = ['PULocationID', 'DOLocationID']

def read_data(filename):
    df = pd.read_parquet(filename)
    
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df


year = 2023
month = 4

df = read_data(f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-%02d.parquet"%month)


dicts = df[categorical].to_dict(orient='records')
X_val = dv.transform(dicts)
y_pred = model.predict(X_val)

print(np.mean(y_pred))
df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')


df_result = pd.DataFrame({"ride id": df["ride_id"],
             "duration": y_pred})


output_file = "../../data/hw03_df_result.parquet"


df_result.to_parquet(
    output_file,
    engine='pyarrow',
    compression=None,
    index=False
)



