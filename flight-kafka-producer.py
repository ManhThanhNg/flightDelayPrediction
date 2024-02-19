import time

import psycopg2
from confluent_kafka import Consumer, KafkaError, SerializingProducer, KafkaException
import simplejson as json
from datetime import datetime
import pandas as pd

import os


from deliveryReport import delivery_report

conf = {
    'bootstrap.servers': 'localhost:9092',
}
consumer = Consumer(conf |
                    {'group.id': 'flight-group',
                     'auto.offset.reset': 'earliest',
                     'enable.auto.commit': False}
                    )
producer = SerializingProducer(conf)

conn = psycopg2.connect("host=localhost dbname=flight user=postgres password=postgres")
cur = conn.cursor()

def insert_flight(conn, cur, row):
    try:
        cur.execute("""
                INSERT INTO delayedflight(Year, Month, DayofMonth,DayOfWeek,DepTime,CRSDepTime,ArrTime,CRSArrTime,UniqueCarrier,FlightNum,TailNum,ActualElapsedTime,CRSElapsedTime,AirTime,ArrDelay,DepDelay,Origin,Dest,Distance,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                    (
                        row['Year'], row['Month'], row['DayofMonth'], row['DayOfWeek'], row['DepTime'], row['CRSDepTime'], row['ArrTime'], row['CRSArrTime'], row['UniqueCarrier'], row['FlightNum'], row['TailNum'], row['ActualElapsedTime'], row['CRSElapsedTime'], row['AirTime'], row['ArrDelay'], row['DepDelay'], row['Origin'], row['Dest'], row['Distance'], row['TaxiIn'], row['TaxiOut'], row['Cancelled'], row['CancellationCode'], row['Diverted'], row['CarrierDelay'], row['WeatherDelay'], row['NASDelay'], row['SecurityDelay'], row['LateAircraftDelay']
                    )
                    )
        conn.commit()
    except Exception as e:
        print("Error during Insert in to delayedflight TABLE: ", e)

if __name__ == "__main__":
    for dirname, _, filenames in os.walk('C:\\Users\\ASUS\\Downloads'):
        for filename in filenames:
            # find the file name DelayedFlights (1).csv
            if filename == 'DelayedFlights (1).csv':
                # read the file
                df = pd.read_csv(os.path.join(dirname, filename))
                # drop the first column
                df = df.drop(df.columns[0], axis=1)
                # for each row in the dataframe insert the row into the delayedFlight table
                for index, row in df.iterrows():
                    try:
                        insert_flight(conn, cur, row)
                        print(f"Inserted {index} out of {len(df)} rows into delayedFlight table.")
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
                    time.sleep(0.5)
    conn.close()
