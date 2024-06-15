import boto3
import os
import argparse
import pickle
import pandas as pd
import numpy as np


def load_model(model_path):
    with open(model_path, 'rb') as f_in:
        dv, model = pickle.load(f_in)
    return dv, model

def read_data(filename, categorical):
    df = pd.read_parquet(filename)
    
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df


def upload_to_s3(local_file, bucket, s3_file):
    s3 = boto3.client('s3')
    s3.upload_file(local_file, bucket, s3_file)

def main(year, month):
    # Your existing code to load the model and read data
    dv, model = load_model('/app/model.bin')
    categorical = ['PULocationID', 'DOLocationID']
    
    df = read_data(f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-%02d.parquet" % month, categorical)

    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = model.predict(X_val)

    print(year, month)
    print("Mean predicted duration:", np.mean(y_pred))
    
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')
    
    df_result = pd.DataFrame({"ride id": df["ride_id"],
                              "duration": y_pred})
    
    
    output_file = f"hw03_df_result_{year}_%02d.parquet"%month
    s3_file_path = f"data/{output_file}"

    print("saving result to file: ", output_file)
    df_result.to_parquet(output_file, engine='pyarrow', compression=None, index=False)
    
    # Upload the file to S3
    bucket_name = os.getenv('S3_BUCKET_NAME')
    upload_to_s3(output_file, bucket_name, s3_file_path)
    print(f"File uploaded to S3: s3://{bucket_name}/{s3_file_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', type=int, required=True, help='Year of the data')
    parser.add_argument('--month', type=int, required=True, help='Month of the data')

    args = parser.parse_args()
    main(args.year, args.month)
