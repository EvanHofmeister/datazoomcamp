Q1) The correct answer is Kafka Node is responsible to store topics, Group-Id ensures the messages are distributed to associated consumers

Q2) The correct answer is Topic Replication, Ack All 

Q3) The correct answer is Topic Paritioning

Q4) The correct answer is payment_type, vendor_id

Q5) The correct answer is Deserializer Configuration, Bootstrap Server, Group-Id, Offset

Q6) We can use kafka-python instead of the java library as it's easier to follow the concepts and not get hung up on java syntax 

Producer:
``` python
import pandas as pd
import json
import datetime as dt
from time import sleep
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:PORT'])
```

``` python
# Initialize 
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

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
    producer.send(topic="taxi_trips", key=key, value=data)

    # Sleep
    sleep(0.3)
```

Consumer:
``` python
from kafka import KafkaConsumer
import json
import pandas as pd
```

``` python
consumer = KafkaConsumer('trips',
                         group_id='test-consumer-group',
                         bootstrap_servers=['localhost:PORT'],
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                         auto_offset_reset='earliest',
                         enable_auto_commit=False)

message_df = pd.DataFrame()
for message in consumer:
    message_df = message_df.append(message.value, ignore_index=True)

print(message_df['PUlocationID'].value_counts())


```
