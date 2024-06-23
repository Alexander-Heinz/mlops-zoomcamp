import datetime
import time
import random
import logging 
import uuid
import pytz
import pandas as pd
import io
import psycopg

########
import requests
import datetime
import pandas as pd
from evidently import ColumnMapping
from evidently.report import Report
from evidently.metrics import ColumnDriftMetric, DatasetDriftMetric, DatasetMissingValuesMetric, ColumnQuantileMetric, ColumnValuePlot
from joblib import load, dump
from tqdm import tqdm

from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error
#########

from evidently.metrics import ColumnDistributionMetric, RegressionQualityMetric
########

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")

SEND_TIMEOUT = 10
rand = random.Random()

TABLE_NAME = "homework_metrics"

create_table_statement = f"""
drop table if exists {TABLE_NAME};
create table homework_metrics(
	timestamp timestamp,
	value1 integer,
	value2 varchar,
	value3 float
)
"""

# mar_data = pd.read_parquet('../data/green_tripdata_2024-03.parquet')
# # create target
# mar_data["duration_min"] = mar_data.lpep_dropoff_datetime - mar_data.lpep_pickup_datetime
# mar_data.duration_min = mar_data.duration_min.apply(lambda td : float(td.total_seconds())/60)
# # filter out outliers
# mar_data = mar_data[(mar_data.duration_min >= 0) & (mar_data.duration_min <= 60)]
# mar_data = mar_data[(mar_data.passenger_count > 0) & (mar_data.passenger_count <= 8)]
# # data labeling
# target = "duration_min"
# num_features = ["passenger_count", "trip_distance", "fare_amount", "total_amount"]
# cat_features = ["PULocationID", "DOLocationID"]
# train_data = mar_data[:30000]
# val_data = mar_data[30000:]
# model = LinearRegression()  
# model.fit(train_data[num_features + cat_features], train_data[target])
# train_preds = model.predict(train_data[num_features + cat_features])
# train_data['prediction'] = train_preds
# val_preds = model.predict(val_data[num_features + cat_features])
# val_data['prediction'] = val_preds


def prep_db():
	with psycopg.connect("host=localhost port=5432 user=postgres password=example", autocommit=True) as conn:
		res = conn.execute("SELECT 1 FROM pg_database WHERE datname='test'")
		if len(res.fetchall()) == 0:
			conn.execute("create database test;")
		with psycopg.connect("host=localhost port=5432 dbname=test user=postgres password=example") as conn:
			conn.execute(create_table_statement)

def calculate_dummy_metrics_postgresql(curr, date, data):

	regular_report = Report(
			metrics=[
				# DataQualityPreset(),
				ColumnQuantileMetric(column_name="fare_amount", quantile = 0.5)
			],
			timestamp=date
		)
	
	regular_report.run(reference_data=None,
                    current_data=data.loc[data.lpep_pickup_datetime.between(date, date+pd.Timedelta(days = 1), inclusive="left")]#,
                    #column_mapping=column_mapping
					)
	
	report_dict = regular_report.as_dict()
	value3 = report_dict["metrics"][0]["result"]["current"]["value"]
	value1 = rand.randint(0, 1000)
	value2 = str(uuid.uuid4())

	print("inserting:", (TABLE_NAME, date, value1, value2, value3))
	curr.execute(
		"insert into %s (timestamp, value1, value2, value3) values (%s, %s, %s, %s)",
		(TABLE_NAME, date, value1, value2, value3)
	)

def main():
	mar_data = pd.read_parquet('../data/green_tripdata_2024-03.parquet')
	# create target
	mar_data["duration_min"] = mar_data.lpep_dropoff_datetime - mar_data.lpep_pickup_datetime
	mar_data.duration_min = mar_data.duration_min.apply(lambda td : float(td.total_seconds())/60)
	# filter out outliers
	mar_data = mar_data[(mar_data.duration_min >= 0) & (mar_data.duration_min <= 60)]
	mar_data = mar_data[(mar_data.passenger_count > 0) & (mar_data.passenger_count <= 8)]
	daterange = mar_data["lpep_pickup_datetime"].dt.to_period('D').unique().to_timestamp()
	prep_db()
	last_send = datetime.datetime.now() - datetime.timedelta(seconds=10)
	with psycopg.connect("host=localhost port=5432 dbname=test user=postgres password=example", autocommit=True) as conn:
		for date in daterange:
			with conn.cursor() as curr:
				calculate_dummy_metrics_postgresql(curr, date, mar_data)
			new_send = datetime.datetime.now()
			seconds_elapsed = (new_send - last_send).total_seconds()
			if seconds_elapsed < SEND_TIMEOUT:
				time.sleep(SEND_TIMEOUT - seconds_elapsed)
			while last_send < new_send:
				last_send = last_send + datetime.timedelta(seconds=0.1)
			logging.info("data sent: ")

if __name__ == '__main__':
	main()