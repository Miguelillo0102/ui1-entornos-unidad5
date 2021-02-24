from pyspark.sql import SparkSession

from pyspark.sql.functions import expr, to_timestamp, window
from pyspark.sql.types import *
import sys
folder=sys.argv[1]

schema = StructType( [StructField('bikeID', LongType()),
StructField('typeEvent', StringType()), StructField('time', StringType()),
StructField('stationID', LongType()), StructField('stationName', StringType()),
StructField('latitude', DoubleType()), StructField('longitude', DoubleType()),
StructField('userType', StringType()), StructField('birthYear', IntegerType()),
StructField('gender', IntegerType())
])

spark = SparkSession.builder.master("local[2]").appName("Gender trips").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
bike_folder_stream=spark.readStream.csv(folder, schema=schema, sep='\t', header=True, inferSchema=False, emptyValue='')
bike_time_stream=bike_folder_stream.withColumn('time', to_timestamp('time','yyyy-MM-dd HH:mm:ss'))
grouped_stream=bike_time_stream.groupBy(window(bike_time_stream.time,'60 minutes')).count()
query=grouped_stream.writeStream.format("console").outputMode("complete").option("truncate",False).start()
query.awaitTermination()
