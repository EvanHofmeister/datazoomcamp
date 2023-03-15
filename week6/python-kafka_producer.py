from kafka import KafkaConsumer
import json
import pandas as pd
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
