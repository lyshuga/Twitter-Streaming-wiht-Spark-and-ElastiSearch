import os

os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--jars /home/mykola/elasticsearch-hadoop-7.12.0/dist/elasticsearch-hadoop-7.12.0.jar pyspark-shell'


from pyspark import SparkContext, SQLContext

sc = SparkContext(appName="PythonSparkStreaming")
import json

es_write_conf = {

# specify the node that we are sending data to (this should be the master)
"es.nodes" : 'localhost',

# specify the port in case it is not the default port
"es.port" : '9200',

# specify a resource in the form 'index/doc-type'
"es.resource" : 'testindex/testdoc',

# is the input JSON?
"es.input.json" : "yes",

# is there a field in the mapping that should be used to specify the ES document ID
"es.mapping.id": "doc_id",

# 'es.index.auto.create': 'true',

'es.index.read.missing.as.empty': "yes"

}

data = [
{'some_key': 'some_value', 'doc_id': 123},
{'some_key': 'some_value', 'doc_id': 456},
{'some_key': 'some_value', 'doc_id': 789}
]


# sqlContext = SQLContext(sc)
# df = sqlContext.createDataFrame(rdd)

# df.write.format("org.elasticsearch.spark.sql")\
#     .option("es.resource", "<koka/test>")\
#     .option("es.nodes", "localhost:9200").save()

rdd = sc.parallelize(data)

def format_data(data):
    return (data['doc_id'], json.dumps(data))

rdd = rdd.map(lambda x: format_data(x))

rdd.saveAsNewAPIHadoopFile(
    path='-',
    outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
    keyClass="org.apache.hadoop.io.NullWritable",
    valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",

    # critically, we must specify our `es_write_conf`
    conf=es_write_conf)

