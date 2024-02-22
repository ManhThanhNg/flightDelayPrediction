import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType
from pyspark.sql.functions import *
from pyspark.sql.functions import count, when, col
from config import KAFKA_BROKER_HOST

spark = (SparkSession.builder
         .appName("Realtime Flight")
         # .master("spark://127.0.0.1:7077")
         .master("local")
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
         .config('spark.jars', 'D:\\Coding\\FlightDelaysPredict\\postgresql-42.7.1.jar')  # add postgresql driver
         .config("spark.sql.adaptive.enabled", "false")
         .getOrCreate())
flight_schema = StructType([
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("dayofmonth", IntegerType(), True),
    StructField("crsdeptime", IntegerType(), True),
    StructField("crsarrtime", IntegerType(), True),
    StructField("uniquecarrier", StringType(), True),
    StructField("origin", StringType(), True),
    StructField("dest", StringType(), True),
    StructField("distance", IntegerType(), True),
    StructField("predict", StringType(), True),
])

if __name__=="__main__":
    flight_df = (spark.readStream
                 .format("kafka")
                 .option("kafka.bootstrap.servers", f"{KAFKA_BROKER_HOST}:9092")  # kafka broker
                 .option("subscribe", "flight-prediction")  # topic
                 .option("startingOffsets", "earliest")  # read from beginning of the stream
                 .load()
                 .selectExpr("CAST(value AS STRING)")  # cast value column to string
                 .select(from_json(col("value"), flight_schema).alias('data')) # convert json to columns
                 .select("data.*")
                 )

    # Data process: group by origin and dest, and count the number of flights, number & type of predict
    origin_dest_count = flight_df.groupBy('origin', 'dest').agg(
        count('origin').alias('flight_count'),
        count(when(col('predict') == 'On Time', True)).alias('ontime_count'),
        count(when(col('predict') == 'Delayed > 60 Mins', True)).alias('delayed_60_count'),
        count(when(col('predict') == 'Delayed > 15 Mins', True)).alias('delayed_15_count'),
        count(when(col('predict') == 'Cancel or Diverted', True)).alias('cancel_diverted_count')
    )

    # query = (origin_dest_count
    #          .writeStream
    #          .outputMode("update")    # complete: all the counts are written to the sink
    #          .format("console")
    #          .option("checkpointLocation", "D:\\Coding\\FlightDelaysPredict\\checkpoint")
    #          .outputMode("update")
    #          .start())
    # query.awaitTermination()
    # write stream to origin_dest_count topic in kafka
    query = (origin_dest_count.selectExpr("to_json(struct(*)) AS value")
             .writeStream
             .format("kafka")
             .option("kafka.bootstrap.servers", f"{KAFKA_BROKER_HOST}:9092")
             .option("topic", "origin_dest_count")
             .option("checkpointLocation", "D:\\Coding\\FlightDelaysPredict\\checkpoint")
             .outputMode("update")
             .start())
    query.awaitTermination()