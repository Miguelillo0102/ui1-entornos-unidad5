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
spark = SparkSession.builder.master("local[2]").appName("Socket example").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
bike_folder_stream=spark.readStream.csv(folder, schema=schema, sep='\t', header=True, inferSchema=False, emptyValue='')
oldest_user_stream=bike_folder_stream.selectExpr('min(birthYear)')
query=oldest_user_stream.writeStream.format("console").outputMode("complete").start()
query.awaitTermination()
