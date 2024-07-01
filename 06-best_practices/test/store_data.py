import os
import pandas as pd
from datetime import datetime

def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)

# Create the dataframe as in Q3
data = [
    (None, None, dt(1, 1), dt(1, 10)),
    (1, 1, dt(1, 2), dt(1, 10)),
    (1, None, dt(1, 2, 0), dt(1, 2, 59)),
    (3, 4, dt(1, 2, 0), dt(2, 2, 1)),      
]

columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
df_input = pd.DataFrame(data, columns=columns)

# Set up environment variables and paths
year = 2023
month = 1
input_file_pattern = os.getenv('INPUT_FILE_PATTERN', 's3://nyc-duration-test/in/{year:04d}-{month:02d}.parquet')
output_file_pattern = os.getenv('OUTPUT_FILE_PATTERN', 's3://nyc-duration-test/out/{year:04d}-{month:02d}.parquet')
s3_endpoint_url = os.getenv('S3_ENDPOINT_URL', 'http://localhost:4566')

input_file = input_file_pattern.format(year=year, month=month)
output_file = output_file_pattern.format(year=year, month=month)

options = {
    'client_kwargs': {
        'endpoint_url': s3_endpoint_url
    }
}

# Save the dataframe to S3
df_input.to_parquet(
    input_file,
    engine='pyarrow',
    compression=None,
    index=False,
    storage_options=options
)