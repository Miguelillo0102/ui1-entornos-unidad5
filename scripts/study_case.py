from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, to_timestamp, window
from pyspark.sql.types import *
import sys
folder=sys.argv[1]
stations_file=sys.argv[2]
output_folder=sys.argv[3]

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

schema_stations = StructType( [StructField('stationID', LongType()),
StructField('latitude',DoubleType()),
StructField('longitude', DoubleType()),
StructField('neighbourhood',StringType())] )

spark = SparkSession.builder.master("local[2]").appName("Output example").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

stations_df=spark.read.csv(path=stations_file, sep='\t', inferSchema=False, schema=schema_stations, header=False, emptyValue='').\
selectExpr('stationID','neighbourhood')

trips_df=spark.readStream.csv(path=folder, sep='\t', inferSchema=False, schema=schema, header=True, emptyValue='').\
withColumn('time',to_timestamp('time','yyyy-MM-dd HH:mm:ss')).\
withWatermark('time', '5 minutes').\
dropDuplicates(['bikeID','time']).\
selectExpr('bikeID','typeEvent','time','stationID')

full_df=trips_df.join(stations_df, trips_df.stationID==stations_df.stationID)
grouped_df=full_df.groupBy( window('time','2 hours'), col('neighbourhood'), col('typeEvent') ).count()
query = grouped_df.writeStream.format("json").option("path", output_folder).option("checkpointLocation", "/home/alumno/checkpoint").outputMode("append").start()

query.awaitTermination()




