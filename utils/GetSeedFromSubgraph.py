# This MapReduce script extracts the seed from subgraphs, for further analysis
# Before executing this script, please make sure you have subgraphs data in the source folder
import os
import sys
import unicodedata
import json
import re


INPUT_FILE_PATH = "../subgraphs/data/red"
OUTPUT_FOLDER_PATH = "../subgraphs/data/seeds"


def spark_initiator():
	from pyspark import SparkContext
	from pyspark import SparkConf
	conf = SparkConf().setAppName("GetSeedFromSubgraph")
	sc_object = SparkContext(conf=conf)
	return sc_object


def flat_mapper_nx(line):
	import os
	import sys
	(seed,graph,vector) = line.split('\t')
	yield (seed)


def main():
	sc = spark_initiator()
	rdd = sc.textFile(INPUT_FILE_PATH).filter(lambda line: len(line) > 1)
	result = rdd.flatMap(lambda x: flat_mapper_nx(x)).repartition(1)
	result.saveAsTextFile(OUTPUT_FOLDER_PATH)


if __name__ == "__main__":
	main()
