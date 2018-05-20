# This script generates a list of users that appear in the global graph.
import os
import sys
import unicodedata
import json
import re

INPUT_FOLDER_PATH = "data/global/UserEdges"

OUTPUT_FOLDER_PATH = "data/global/Users"


def spark_initiator():
	from pyspark import SparkContext
	from pyspark import SparkConf
	conf = SparkConf().setAppName("GenerateUserNodesForGlobalPR")
	sc_object = SparkContext(conf=conf)
	return sc_object


def splitline(line):
	line = re.sub('[(]',"", line)
	line = re.sub('[)]', "", line)
	line = line.replace("'","").replace(",","")
	line = line.split(' ')
	return line[0],line[1]


def main():
	sc = spark_initiator()
	rdd = sc.textFile(INPUT_FOLDER_PATH).filter(lambda x: len(x) > 1).flatMap(splitline)
	# ZipwithUniqueID gives each user a unique ID 
	rdd2 = rdd.distinct().map(lambda line: str(line)).zipWithUniqueId()
	rdd2.saveAsTextFile(OUTPUT_FOLDER_PATH)

if __name__ == "__main__":
	main()

