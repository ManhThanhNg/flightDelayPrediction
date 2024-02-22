from confluent_kafka import Consumer, SerializingProducer, KafkaError
import os
import sys

from deliveryReport import delivery_report

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
import simplejson as json
from pyspark import SQLContext, SparkContext
from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType
from pyspark.sql.functions import *
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegressionModel

spark = (SparkSession.builder
         .appName("Realtime Flight")
         # .master("spark://127.0.0.1:7077")
         .master("local")
         # .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
         .config('spark.jars', 'D:\\Coding\\FlightDelaysPredict\\postgresql-42.7.1.jar')  # add postgresql driver
         .config("spark.sql.adaptive.enabled", "false")
         .getOrCreate())

conf = {
    'bootstrap.servers': 'localhost:9092',
}
consumer = Consumer(conf |
                    {'group.id': 'flight-group',
                     'auto.offset.reset': 'earliest',
                     'enable.auto.commit': False}
                    )
producer = SerializingProducer(conf)


if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("Realtime Flight")
             # .master("spark://127.0.0.1:7077")
             .master("local")
             # .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
             .config('spark.jars', 'D:\\Coding\\FlightDelaysPredict\\postgresql-42.7.1.jar')  # add postgresql driver
             .config("spark.sql.adaptive.enabled", "false")
             .getOrCreate())

    consumer.subscribe(['flight'])
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                # turn message into a dataframe
                message = json.loads(msg.value().decode('utf-8'))
                # # print(msg.value().decode('utf-8'))
                # print(type(msg.value().decode('utf-8')))
                # print(message.items())
                # creaet df that have these column: year|month|dayofmonth|crsdeptime|crsarrtime|uniquecarrier|origin|dest|distance
                df = spark.createDataFrame([Row(**message)])
                indexer = StringIndexer(inputCol="uniquecarrier", outputCol="uniquecarrier_index").fit(df).transform(df)
                indexer = StringIndexer(inputCol="origin", outputCol="origin_index").fit(indexer).transform(indexer)
                indexer = StringIndexer(inputCol="dest", outputCol="dest_index").fit(indexer).transform(indexer)
                assembler = VectorAssembler(inputCols=["year", "month", "dayofmonth", "crsdeptime", "crsarrtime", "uniquecarrier_index", "origin_index", "dest_index", "distance"], outputCol="features")
                flight_assembled = assembler.transform(indexer)
                lr = LogisticRegressionModel.load("D:\\Coding\\FlightDelaysPredict\\models\\logistic_regression_model")
                predictions = lr.transform(flight_assembled)
                # create new df with column year|month|dayofmonth|crsdeptime|crsarrtime|uniquecarrier|origin|dest|distance|uniquecarrier_index|origin_index|dest_index| with predict column that describe the value in prediction
                predictions_result = predictions.select("year", "month", "dayofmonth", "crsdeptime", "crsarrtime", "uniquecarrier", "origin", "dest", "distance", expr("""
                    case
                        when prediction = 3.0 then 'Cancel or Diverted'
                        when prediction = 2.0 then 'Delayed > 60 Mins'
                        when prediction = 1.0 then 'Delayed > 15 Mins'
                        when prediction = 0.0 then 'On Time'
                    end as predict
                """))
                try:
                    producer.produce(
                        topic='flight-prediction',
                        key=msg.key(),
                        value=predictions_result.toJSON().collect()[0],
                        on_delivery=delivery_report
                    )
                    producer.poll(0)
                    print(f"Produced {msg.key()} out of {msg.topic()} rows into flight-prediction topic.")
                except Exception as e:
                    print(e)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        producer.close()