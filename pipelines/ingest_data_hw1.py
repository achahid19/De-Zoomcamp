import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import os
import math

CHUNK_SIZE = 100000

def ingest_data():
	# run sql engine
	engine = create_engine(f'postgresql://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}@{os.getenv("DB_HOST")}:{os.getenv("DB_PORT")}/{os.getenv("DB_NAME")}')
	

	# get parquet data
	trip_data_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet"
	trip_data_df = pd.read_parquet(trip_data_url)
	
	# split into chunks manually
	num_chunks = math.ceil(len(trip_data_df) / CHUNK_SIZE)
	trip_data_chunks = [trip_data_df.iloc[i*CHUNK_SIZE:(i+1)*CHUNK_SIZE] for i in range(num_chunks)]

	# get zones dataset
	zones_data_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
	zones_data_df = pd.read_csv(zones_data_url, iterator=True, chunksize=CHUNK_SIZE)

	first = True
	# load trip data to sql
	for chunk in tqdm(trip_data_chunks):
		# load parquet chunk to sql
		if first:
			first = False
			chunk.head(n=0).to_sql(name='green_taxi_data', con=engine, if_exists='replace')
			print('Created')
		# insert data
		chunk.to_sql(name='green_taxi_data', con=engine, if_exists='append')
		print(f'Inserted: {len(chunk)} records')

	first = True

	# load zones data to sql
	for chunk in tqdm(zones_data_df):
		# if first create table
		if first:
			first = False
			chunk.head(n=0).to_sql(name='taxi_zones', con=engine, if_exists='replace')
			print('Created')
		# insert data
		chunk.to_sql(name='taxi_zones', con=engine, if_exists='append')
		print(f'Inserted: {len(chunk)} records')

if __name__ == '__main__':
	ingest_data()	
