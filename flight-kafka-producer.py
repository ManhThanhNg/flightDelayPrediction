import time

import psycopg2
from confluent_kafka import Consumer, KafkaError, SerializingProducer, KafkaException
import simplejson as json
from datetime import datetime
import pandas as pd

import os

from config import KAFKA_BROKER_HOST
from deliveryReport import delivery_report

conf = {
    'bootstrap.servers': f'{KAFKA_BROKER_HOST}:9092',
}
consumer = Consumer(conf |
                    {'group.id': 'flight-group',
                     'auto.offset.reset': 'earliest',
                     'enable.auto.commit': False}
                    )
producer = SerializingProducer(conf)

if __name__ == "__main__":
    for dirname, _, filenames in os.walk('C:\\Users\\ASUS\\Downloads'):
        for filename in filenames:
            # find the file name DelayedFlights (1).csv
            if filename == 'DelayedFlights (1).csv':
                # read the file
                df = pd.read_csv(os.path.join(dirname, filename))
                # drop the first column
                df = df.drop(df.columns[0], axis=1)
                # just leave column year|month|dayofmonth|crsdeptime|crsarrtime|uniquecarrier|origin|dest|distance
                df = df[['Year', 'Month', 'DayofMonth', 'CRSDepTime', 'CRSArrTime', 'UniqueCarrier', 'Origin', 'Dest', 'Distance']]
                # change column name to lowercase
                df.columns = df.columns.str.lower()
                # for each row in the dataframe insert the row into the delayedFlight table
                for index, row in df.iterrows():
                    try:
                        producer.produce(
                            topic='flight',
                            key=str(index),
                            value=row.to_json(),
                            on_delivery=delivery_report
                        )
                        producer.flush(0)
                        print(f"Produced {index} out of {len(df)} rows into flight topic.")
                    except Exception as e:
                        print(e)
                    time.sleep(5)