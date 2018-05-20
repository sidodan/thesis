# This script uses "GraphFrames" package in order to count the number determine the in-degrees and out-degrees for each user node
# Please make sure you have the list of Users and User Edges before executing this script. 
from graphframes import *
from pyspark import SparkContext, SparkConf
import re

VERTICES_INPUT_PATH = "data/global/Users"
EDGES_INPUT_PATH = "data/global/UserEdges"
INDEGREES = "data/global/InDegrees"
OUTDEGREES = "data/global/OutDegrees"

def spark_initiator():
	from pyspark import SparkContext
	from pyspark.sql import SQLContext
	conf = SparkConf().setAppName("DegreesAnalysisWithGrpahFrames")
	sc_object = SparkContext(conf=conf)
	sqlContext_object = SQLContext(sc_object)
	return sc_object, sqlContext_object


def split(line2):
	line2 = re.sub('[(]', "", line2)
	line2 = re.sub('[)]', "", line2)
	line2 = line2.replace("'", "").replace(",", "")
	line2 = line2.split(' ')
	return line2


(sc, sqlContext) = spark_initiator()
vertices_rdd = sc.textFile(VERTICES_INPUT_PATH).filter(lambda line: len(line) > 1).map(split)
v = vertices_rdd.toDF(["id", "nodeid"])
edges_rdd = sc.textFile(EDGES_INPUT_PATH).filter(lambda line: len(line) > 1).map(split)
e = edges_rdd.toDF(["src", "dst", "weight"])
g = GraphFrame(v, e)
g.inDegrees.write.json(INDEGREES)
g.outDegrees.write.json(OUTDEGREES)
