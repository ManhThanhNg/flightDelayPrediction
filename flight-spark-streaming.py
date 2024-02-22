# import os
# import sys
# os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType
from pyspark.sql.functions import *
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
                 .option("kafka.bootstrap.servers", "localhost:9092")  # kafka broker
                 .option("subscribe", "flight-prediction")  # topic
                 .option("startingOffsets", "earliest")  # read from beginning of the stream
                 .load()
                 .selectExpr("CAST(value AS STRING)")  # cast value column to string
                 .select(from_json(col("value"), flight_schema).alias('data')) # convert json to columns
                 .select("data.*")
                 )

    # write stream to console
    query = (flight_df.writeStream
             .outputMode("append")
             .format("console")
             .option("checkpointLocation", "D:\\Coding\\FlightDelaysPredict\\checkpoint")
             .outputMode("update")
             .start())
    query.awaitTermination()