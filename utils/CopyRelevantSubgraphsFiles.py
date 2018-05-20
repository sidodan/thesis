# This MapReduce script copies subgraph output files between folders
# Please make sure you have a lsit of the subpopulation users you wish to copy before running this script
import os
import sys
import unicodedata
import json
import re


INPUT_FILE_PATH = "../subgraphs/data/all"
OUTPUT_FOLDER_PATH = "../subgraphs/data/red"
SEED_FILE_PATH = "../input/users_files/orig/subpopulation_users_list.txt"

__seeds__ = {}


def spark_initiator():
	from pyspark import SparkContext
	from pyspark import SparkConf
	conf = SparkConf().setAppName("CopySubgraphs")
	sc_object = SparkContext(conf=conf)
	return sc_object


def load_seed(rdd):
	for line in rdd.take(rdd.count()):
		key = int(line.split('\t')[0])
		__seeds__[key] = set([])
		seeds_list = line.split('\t')[1]
		seeds = seeds_list.split(';')
		for seed in seeds:
			__seeds__[key].add(seed)

			
def flat_mapper(line):
	import os
	import sys
	(seed,graph,vector) = line.split('\t')
	if seed in __seeds__[1]:
		yield (line)


def main():
	sc = spark_initiator()
	rdd_seeds = sc.textFile(SEED_FILE_PATH)
	load_seed(rdd_seeds)
	rdd = sc.textFile(INPUT_FILE_PATH).filter(lambda line: len(line) > 1)
	result = rdd.flatMap(lambda x: flat_mapper(x))
	result.saveAsTextFile(OUTPUT_FOLDER_PATH)


if __name__ == "__main__":
	main()
