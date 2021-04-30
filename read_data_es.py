import os

os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--jars /home/mykola/elasticsearch-hadoop-7.12.0/dist/elasticsearch-hadoop-7.12.0.jar pyspark-shell'

es_read_conf = {

# specify the node that we are sending data to (this should be the master)
"es.nodes" : 'localhost',

# specify the port in case it is not the default port
"es.port" : '9200',
#
# # specify a resource in the form 'index/doc-type'
"es.resource" : 'testindex/testdoc',

"es.query": "?q=me*",
"spark.es.nodes.wan.only": "true",

'es.index.auto.create': 'true',
'es.index.read.missing.as.empty': 'yes'

}

from pyspark import SparkContext, SparkConf, SQLContext



conf = SparkConf().setAppName("ESTest")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


es_read_conf = {
    "es.nodes" : "localhost",
    "es.port" : "9200",
    "es.resource" : "testindex/testdoc",
    "es.query": "?q=*",
    "es.index.read.missing.as.empty": "true"
}

es_rdd = sc.newAPIHadoopRDD(
    inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
    keyClass="org.apache.hadoop.io.NullWritable",
    valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
    conf=es_read_conf)

a = sqlContext.createDataFrame(es_rdd)
a.collect()

a.show()


# sc = SparkContext(appName="PythonSparkStreaming")
# es_rdd = sc.newAPIHadoopRDD(
# inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
# keyClass="org.apache.hadoop.io.NullWritable",
# valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
# conf=es_read_conf)
#
