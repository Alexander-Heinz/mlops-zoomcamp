#!/usr/bin/env python
# coding: utf-8

import sys
import pickle
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()


def get_input_path(year, month):
    default_input_pattern = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
    print("using input file: ", input_pattern)
    return input_pattern.format(year = year, month = month)

def get_output_path(year, month):
    default_output_pattern = 's3://nyc-duration-test/taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet'
    output_pattern = os.getenv('OUTPUT_FILE_PATTERN', default_output_pattern)
    return output_pattern.format(year=year, month=month)


def prepare_data(df, categorical):
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')

    return df

def read_data(filename):
    s3_endpoint_url = os.getenv('S3_ENDPOINT_URL')
    if s3_endpoint_url:
        print("using s3 endpoint!")
        options = {
            'client_kwargs': {
                'endpoint_url': s3_endpoint_url
            }
        }
        df = pd.read_parquet(filename, storage_options=options)
    else:
        df = pd.read_parquet(filename)
    return df


def main(year, month, categorical = ['PULocationID', 'DOLocationID']): 

    print("running for... \n")
    print("year ", year)
    print("month ", month)

    input_file = get_input_path(year, month)
    output_file = get_output_path(year, month)

    # Ensure the output directory exists
    # os.makedirs(os.path.dirname(output_file), exist_ok=True)


    with open('model.bin', 'rb') as f_in:
        dv, lr = pickle.load(f_in)

    df = read_data(input_file)
    df = prepare_data(df, categorical)

    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')


    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)


    print('predicted mean duration:', y_pred.mean())
    print('predicted sum duration:', y_pred.sum())


    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['predicted_duration'] = y_pred


    df_result.to_parquet(output_file, engine='pyarrow', index=False)



if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: ./predict.py <year> <month>")
        sys.exit(1)
    
    year = int(sys.argv[1])
    month = int(sys.argv[2])
    
    main(year, month)
