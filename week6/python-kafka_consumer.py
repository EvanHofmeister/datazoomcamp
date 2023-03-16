# Import packages
import pandas as pd
import numpy as np
import json
import datetime as dt
from time import sleep
from kafka import KafkaProducer
import logging
import sys

root = logging.getLogger()
root.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)

producer = KafkaProducer(bootstrap_servers=['localhost:PORT'])
# Initialize 
path = r'/Users/evanhofmeister/data-zoom-camp/Data'/green_tripdata_2019-01.csv.gz'

for count, chunk in enumerate(pd.read_csv(path, compression="gzip", chunksize=10000)):
    # Fix format

    chunk["pickup_datetime"] = pd.to_datetime(chunk["pickup_datetime"])
    chunk["dropoff_datetime"] = pd.to_datetime(chunk["dropOff_datetime"])

    chunk = chunk[['PUlocationID', 'pickup_datetime', 'dropoff_datetime']]

    key = str(count).encode()

    chunkd = chunk.to_dict()

    # Encode the dictionary into JSON 
    data = json.dumps(chunkd, default=str).encode('utf-8')

    # Send the data to Kafka
    producer.send(topic="trips", key=key, value=data)

    # Sleep
    sleep(0.3)
