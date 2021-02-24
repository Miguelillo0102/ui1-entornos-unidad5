from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import *
import sys
folder=sys.argv[1]
schema = StructType( [StructField('bikeID', LongType()),
    StructField('typeEvent', StringType()),
    StructField('time', StringType()),
    StructField('stationID', LongType()),
    StructField('stationName', StringType()),
    StructField('latitude', DoubleType()),
    StructField('longitude', DoubleType()),
    StructField('userType', StringType()),
    StructField('birthYear', IntegerType()),
    StructField('gender', IntegerType())
    ])
spark = SparkSession.builder.master("local[2]").appName("Gender trips").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
bike_folder_stream=spark.readStream.csv(folder, schema=schema, sep='\t', header=True, inferSchema=False, emptyValue='')
gender_trips_stream = bike_folder_stream.filter(expr('gender>0'))
gr_gender_trips_stream = gender_trips_stream.groupBy('gender')
gender_most_trips=gr_gender_trips_stream.agg(expr('count(gender)'))
query=gender_most_trips.writeStream.format("console").outputMode("complete").start()
query.awaitTermination()
