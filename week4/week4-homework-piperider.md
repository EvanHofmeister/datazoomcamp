For this homework we can follow the below steps to run Piperider with DuckDB:

### 1. Create local postgres database to house green, yellow, and FHV taxi data

First we need to install postgres and pgcli

```bash
$ brew install postgresql@14
```

```bash
$ brew install pgcli
```

Then we can create a postgres user and database:
```bash
create postgres user and database:
postgres-> CREATE DATABASE ny_taxi;
postgres-> GRANT ALL PRIVILEGES ON DATABASE ny_taxi TO myuser;
ALTER ROLE myuser INHERIT;
GRANT ALL ON SCHEMA ny_taxi TO myuser;
```

Now we can run the below python script to grab the parquet files and load them into the postgres database:

```python
from pathlib import Path
import pandas as pd
from prefect import flow, task
# from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from time import time

#sqlalchemy dependencies
from sqlalchemy import create_engine
import sys
import subprocess

# implement pip as a subprocess:
# subprocess.check_call([sys.executable, '-m', 'pip', 'install', 
# '<psycopg2-binary>'])

@task(retries=3, log_prints=True)
def fetch(dataset_url: str) -> pd.DataFrame:
    print(dataset_url)
    # df = pd.read_csv(dataset_url, compression='gzip')
    df = pd.read_csv(dataset_url, compression='gzip', encoding='ISO-8859-1')
    return df


@task(log_prints=True)
def clean(color: str, df: pd.DataFrame) -> pd.DataFrame:

    if color == "yellow":
        """Fix dtype issues"""
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    if color == "green":
        """Fix dtype issues"""
        df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
        df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
        df["trip_type"] = df["trip_type"].astype('Int64')

    if color == "yellow" or color == "green":
        df["VendorID"] = df["VendorID"].astype('Int64')
        df["RatecodeID"] = df["RatecodeID"].astype('Int64')
        df["PULocationID"] = df["PULocationID"].astype('Int64')
        df["DOLocationID"] = df["DOLocationID"].astype('Int64')
        df["passenger_count"] = df["passenger_count"].astype('Int64')
        df["payment_type"] = df["payment_type"].astype('Int64')

    if color == "fhv":
        """Rename columns"""
        df.rename({'dropoff_datetime':'dropOff_datetime'}, axis='columns', inplace=True)
        df.rename({'PULocationID':'PUlocationID'}, axis='columns', inplace=True)
        df.rename({'DOLocationID':'DOlocationID'}, axis='columns', inplace=True)

        """Fix dtype issues"""
        df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
        df["dropOff_datetime"] = pd.to_datetime(df["dropOff_datetime"])

        df["PUlocationID"] = df["PUlocationID"].astype('Int64')
        df["DOlocationID"] = df["DOlocationID"].astype('Int64')

    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(color: str, df: pd.DataFrame, dataset_file: str, engine) -> Path:   
    t_start = time()
    df.to_sql(name=f"{color}_taxi_data", con=engine, if_exists='append')
    t_end = time()
    print('insert another chunk..., took %.3f second' % (t_end - t_start))

@flow()
def web_to_gcs() -> None:

    # color = "yellow"
    # years = [2019,2020]
    color = "green"
    years = [2019,2020]
    # color = "fhv"
    # years = [2019]
    

    engine = create_engine('postgresql://localhost/ny_taxi')
    engine.connect()
    connection = engine.raw_connection()
    cursor = connection.cursor()
    command = "DROP TABLE IF EXISTS {};".format(f"{color}_taxi_data")
    cursor.execute(command)
    connection.commit()
    cursor.close()

    for year in years:
        for month in range(1, 13):
            dataset_file = f"{color}_tripdata_{year}-{month:02}"
            dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

            df = fetch(dataset_url)
            df_clean = clean(color, df)
            path = write_local(color, df_clean, dataset_file, engine)

if __name__ == "__main__":
    web_to_gcs()
```

Next, in a seperate terminal instance we can start pgcli:

```bash
pgcli -h localhost -u myuser -d ny_taxi
pgcli -h localhost -u evanhofmeister -d ny_taxi
```

Then we can create a python enviornment:
```bash
conda create -n py39_full anaconda python=3.9
conda activate py39_full 
```

Install a few dependencies 
```bash
pip install -U prefect
pip install psycopg2-binary
```

![pgcli_table_count.png](pgcli_table_count.png.png)



Next we can step through the below procedures to run dbt and piperider:

1. Fork this repo
2. Clone your forked repo

	```bash
	git clone <your-repo-url>
	cd taxi_rides_ny_duckdb
	```

3. Download the DuckDB database file

	```bash
	wget https://dtc-workshop.s3.ap-northeast-1.amazonaws.com/nyc_taxi.duckdb
	``` 
4. Set up a new venv

	```bash
	python -m venv ./venv
	source ./venv/bin/activate
	```
5. Update pip and install the neccessary dbt packages and PipeRider

	```bash
	pip install -U pip
	pip install dbt-core dbt-duckdb 'piperider[duckdb]'
	```
6. Create a new branch to work on

	```bash
	git switch -c data-modeling
	```
	
7. Install dbt deps and build dbt models

	```bash
	dbt deps
	dbt build
	```
	
8. Initialize PipeRider

	```bash
	piperider init
	```
	
9. Check PipeRider settings

	```bash
	piperider diagnose
	```
	
### 2. Run PipeRider and data model changes
	
1. Run PipeRider

	```bash
	piperider run
	```
	
	PipeRider will profile the database and output the path to your data report, e.g.
	
	```
	Generating reports from: /project/path/.piperider/outputs/latest/run.json
	Report generated in /project/path/.piperider/outputs/latest/index.html
	```
	
	View the HTML report to see the full statistical report of your data source.
	



6. The comparison summary also contains a summary of the metric differences between reports.

Q1) From running the below procedure/code we find the correct answer to be 60.1/39.5/0.4

![Q1](HW_Q1.png)

Q2) From running the below procedure/code we find the correct answer to be 61.4M/25K/148.6K

![Q2](HW_Q2.png)

Q3) From running the below procedure/code we find the correct answer to be 2.95/35.43/-23.88/167.3K/181.5M

![Q3](HW_Q3.png)


