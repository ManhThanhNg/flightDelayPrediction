from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType
from pyspark.sql.functions import *
if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("Realtime Flight")
             .master("spark://localhost:7077")
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
             .config('spark.jars', 'D:\Coding\FlightDelaysPredict\postgresql-42.7.1.jar')  # add postgresql driver
             .config("spark.sql.adaptive.enabled", "false")
             .getOrCreate())

    flight_schema = StructType([
        StructField("Year", IntegerType(), True),
        StructField("Month", IntegerType(), True),
        StructField("DayofMonth", IntegerType(), True),
        StructField("DayOfWeek", IntegerType(), True),
        StructField("DepTime", FloatType(), True),
        StructField("CRSDepTime", IntegerType(), True),
        StructField("ArrTime", FloatType(), True),
        StructField("CRSArrTime", IntegerType(), True),
        StructField("UniqueCarrier", StringType(), True),
        StructField("FlightNum", IntegerType(), True),
        StructField("TailNum", StringType(), True),
        StructField("ActualElapsedTime", FloatType(), True),
        StructField("CRSElapsedTime", FloatType(), True),
        StructField("AirTime", FloatType(), True),
        StructField("ArrDelay", FloatType(), True),
        StructField("DepDelay", FloatType(), True),
        StructField("Origin", StringType(), True),
        StructField("Dest", StringType(), True),
        StructField("Distance", IntegerType(), True),
        StructField("TaxiIn", FloatType(), True),
        StructField("TaxiOut", FloatType(), True),
        StructField("Cancelled", IntegerType(), True),
        StructField("CancellationCode", StringType(), True),
        StructField("Diverted", IntegerType(), True),
        StructField("CarrierDelay", FloatType(), True),
        StructField("WeatherDelay", FloatType(), True),
        StructField("NASDelay", FloatType(), True),
        StructField("SecurityDelay", FloatType(), True),
        StructField("LateAircraftDelay", FloatType(), True)
    ])
    # test if spark session is working
    flight_df = (spark.readStream
                 .format("kafka")
                 .option("kafka.bootstrap.servers", "localhost:9092")  # kafka broker
                 .option("subscribe", "flight")  # topic
                 .option("startingOffsets", "earliest")  # read from beginning of the stream
                 .load()
                 .selectExpr("CAST(value AS STRING)")  # cast value column to string
                 .select(from_json(col("value"), flight_schema).alias('data'))  # convert json to columns
                 .select("data.*")
                 )
    # print 5 rows from the dataframe
    flight_df.printSchema()

