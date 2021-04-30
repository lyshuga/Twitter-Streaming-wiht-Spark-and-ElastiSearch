import os
import sys

os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--jars /home/mykola/elasticsearch-hadoop-7.12.0/dist/elasticsearch-hadoop-7.12.0.jar pyspark-shell'



os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
from collections import namedtuple
from textblob import TextBlob
import regex as re

import json

es_write_conf = {

# specify the node that we are sending data to (this should be the master)
"es.nodes" : 'localhost',

# specify the port in case it is not the default port
"es.port" : '9200',

# specify a resource in the form 'index/doc-type'
"es.resource" : 'twitter2/tweets',

# is the input JSON?
"es.input.json" : "yes",

# is there a field in the mapping that should be used to specify the ES document ID
#"es.mapping.id": "doc_id",

# 'es.index.auto.create': 'true',

'es.index.read.missing.as.empty': "yes"

}


sc = SparkContext("local[2]", "NetworkWordCount")

#get access to data  streaming, process in every 1 seconds
ssc = StreamingContext(sc, 10)

#SQL query builder
sqlContext = SQLContext(sc)

# Create a DStream that will connect to hostname:port
# this is where we will be expecting a Twitter streaming connection
lines = ssc.socketTextStream('127.0.0.3', 5555)
#This 'lines' DStream represents the stream of data that will be received from the data server.
#Each record in this DStream is a line of text.



#target = io.open("sentiment.txt", 'w', encoding='utf-8')
def sentiment_score(chat):
    chat = re.sub("(?:(https|http)\s?:\/\/)(\s)*(www\.)?(\s)*((\w|\s)+\.)*([\w\-\s]+\/)*([\w\-]+)((\?)?[\w\s]*=\s*[\w\%&]*)*", " ", chat)
    chat = re.sub("[^A-Za-z]", " ", chat)

    return TextBlob(chat).sentiment.polarity




lines = lines.map(lambda line: {'tweet': line, 'sentiment score': sentiment_score(line)})#(sentiment_score(line), line))

# Print the first ten elements of each RDD generated in this DStream to the console
lines.pprint()

print("LINES --------------------")

idd = 0

def foo(rdd):
	def format_data(data):
	    global idd
	    idd += 1
	    return (idd, json.dumps(data))

	rdd = rdd.map(lambda x: format_data(x))
	print(rdd.collect())
	rdd.saveAsNewAPIHadoopFile(
	    path='-',
	    outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
	    keyClass="org.apache.hadoop.io.NullWritable",
	    valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",

	    # critically, we must specify our `es_write_conf`
	    conf=es_write_conf)

lines.foreachRDD(foo)

#no real processing has started yet
ssc.start()             # Start the computation
print('START ++++++++++++++++++++++')

ssc.awaitTermination()  # Wait for the computation to terminate

print('TERMINATION TTT')
