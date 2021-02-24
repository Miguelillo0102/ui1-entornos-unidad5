from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
spark = SparkSession.builder.master("local[2]").appName("Socket example").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
socket_stream=spark.readStream.format("socket").option("host","localhost").option("port",9999).load()
query=socket_stream.writeStream.format("console").start()
query.awaitTermination()
