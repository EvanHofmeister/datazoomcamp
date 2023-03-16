from kafka import KafkaConsumer
import json
import pandas as pd
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

consumer = KafkaConsumer('trips',
                         group_id=None,
                         bootstrap_servers=['localhost:PORT'],
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                         auto_offset_reset='earliest',
                         enable_auto_commit=False, api_version=(1, 4, 1))

df = pd.DataFrame()
for count, message in enumerate(consumer):
    print('test')
    df = pd.concat([df, pd.DataFrame(message.value)], axis=0)
    logging.info(f"====>>>>>: {str(msg)}")

    
    print(count, df)
        
    # Count frequency of PUlocationID.
# print(df.head())
summary = df['PUlocationID'].value_counts()

print(summary)
