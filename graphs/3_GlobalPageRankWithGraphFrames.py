# This script executes a PageRank algorithm on the entire graph, using GrpahFrames package
# Please make sure you have the list of Users and User Edges before executing this script. 

from graphframes import *
from pyspark import SparkContext, SparkConf
import re

EDGES_INPUT_PATH = "data/global/UserEdges"
VERTICES_INPUT_PATH = "data/global/Users"
EDGES_OUTPUT = "data/global/Edges"
VERTICES_OUTPUT = "data/global/Vertices"
PR_OUTPUT = "data/global/PageRank"


def spark_initiator():
	from pyspark import SparkContext
	from pyspark.sql import SQLContext
	conf = SparkConf().setAppName("GlobalPageRankWithGraphFrames")
	sc_object = SparkContext(conf=conf)
	sqlContext_object = SQLContext(sc_object)
	return sc_object, sqlContext_object


def split(row):
	row = re.sub('[(]', "", row)
	row = re.sub('[)]', "", row)
	row = row.replace("'", "").replace(",", "")
	row = row.split(' ')
	return row

	
(sc, sqlContext) = spark_initiator()

vertices_rdd = sc.textFile(VERTICES_INPUT_PATH).filter(lambda line: len(line) > 1).map(split)
v = vertices_rdd.toDF(["id", "nodeid"])

edges_rdd = sc.textFile(EDGES_INPUT_PATH).filter(lambda line: len(line) > 1).map(split)
e = edges_rdd.toDF(["src", "dst", "weight"])

# Store the global graph
g.vertices.write.json(VERTICES_OUTPUT)
g.edges.write.json(EDGES_OUTPUT)

g = GraphFrame(v, e)

# Repartition is determined based on graph size
# Te algorithm runs for 100 iterations, using a random reset of 0.15 (0.15 chance to jump to a random node on the next move)
result = g.pageRank(resetProbability=0.15, maxIter=100).repartition(1000)
result.vertices.write(PR_OUTPUT)
