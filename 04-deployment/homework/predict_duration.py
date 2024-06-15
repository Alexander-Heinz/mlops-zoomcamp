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

def main(year, month):
    dv, model = load_model('model.bin')
    categorical = ['PULocationID', 'DOLocationID']
    
    df = read_data(f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-%02d.parquet" % month, categorical)

    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = model.predict(X_val)

    print("Mean predicted duration:", np.mean(y_pred))
    
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')
    
    df_result = pd.DataFrame({"ride id": df["ride_id"],
                              "duration": y_pred})
    
    
    # output_file = f"../../data/hw03_df_result_{year}_%02d.parquet"%month
    # df_result.to_parquet(output_file, engine='pyarrow', compression=None, index=False)
    # print("saved result to file: ", output_file)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', type=int, required=True, help='Year of the data')
    parser.add_argument('--month', type=int, required=True, help='Month of the data')

    args = parser.parse_args()
    main(args.year, args.month)
