import socket
import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 1 second.
# The master requires 2 cores to prevent a starvation scenario.

TCP_IP = "localhost"
TCP_PORT = 6100

"""r = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
r.connect((TCP_IP,TCP_PORT))

data = r.recv(1048576).decode()
print(data)
print(sys.getsizeof(data))
print(len(data))
"""
# Create a local StreamingContext with two working thread and batch interval of 1 second
c = SparkContext.getOrCreate()
spr = SparkSession(c)
c.setLogLevel('OFF')
sc = StreamingContext(c, 1)

batch = sc.socketTextStream(TCP_IP, TCP_PORT)
def readBatchStream(rdd):
	df = spr.read.json(rdd)
	df.printSchema()
	df.show()
	
batch.foreachRDD(lambda x : readBatchStream(x))

sc.start()
sc.awaitTermination()
