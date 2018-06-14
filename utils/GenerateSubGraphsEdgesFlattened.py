# This script transforms the edges list into two columns format: seed;edge , and drops the personalization vector
# The aim is to use a column view for analysis proposes.
import os
import sys
import unicodedata
import json
import re

INPUT_FILE_PATH = "../subgraphs/data/red"
OUTPUT_FOLDER_PATH = "../subgraphs/data/edges"


def spark_initiator():
	from pyspark import SparkContext
	from pyspark import SparkConf
	conf = SparkConf().setAppName("GenerateSubgraphsFlattened")
	sc_object = SparkContext(conf=conf)
	return sc_object


def flat_mapper(line):
	(seed, graph, vector) = line.split('\t')
	edges_graph = graph.split(',')
	for edge in edges_graph:
		yield (seed + ' ; ' + edge)


def main():
	sc = spark_initiator()
	rdd = sc.textFile(INPUT_FILE_PATH).filter(lambda line: len(line) > 1)
	result = rdd.flatMap(lambda x: flat_mapper(x))
	result.saveAsTextFile(OUTPUT_FOLDER_PATH)


if __name__ == "__main__":
	main()
