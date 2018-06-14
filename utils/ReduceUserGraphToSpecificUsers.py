# This script takes the graph as an input, and reduces it to a specific set of users. A reduced graph is necessary for visualization needs. 
# Before executing this script, please make sure to have a file including the subpopulation users, so the reduction would succeed.
import re
import os
import sys

INPUT_FOLDER_PATH = "../graphs/USER_GRAPH"
OUTPUT_FOLDER_PATH = "../analysis/output/SpecificUsers"
USERS_TO_INCLUDE_FILE_PATH = "../input/users_files/orig/subpopulation_users.txt"

__users__ = {}

def spark_initiator():
	from pyspark import SparkContext
	from pyspark import SparkConf
	conf = SparkConf().setAppName("Reduce")
	sc_object = SparkContext(conf=conf)
	return sc_object


def load_file_users(rdd):
	for line in rdd.take(rdd.count()):
		key = int(line.split('\t')[0])
		__users__[key] = set([])
		users_string = line.split('\t')[1]
		users = users_string.split(';')
		for user in users:
			__users__[key].add(user)


def flat_mapper(line):
	line = line.strip()
	match = re.search(r'\(\(\'([^\']+)\', \{([^\}]+)\}\), \d+\)', line)
	if match:
		center = match.group(1)
		if center in __users__[1]:
			yield (line)


def main():
	sc = spark_initiator()
	rdd_users = sc.textFile(USERS_TO_INCLUDE_FILE_PATH).filter(lambda line: len(line) > 1)
	load_file_users(rdd_users)
	tweet_rdd = sc.textFile(INPUT_FOLDER_PATH)
	result = tweet_rdd.flatMap(lambda row: flat_mapper(row)).repartition(1)
	result.saveAsTextFile(OUTPUT_FOLDER_PATH)


if __name__ == "__main__":
	main()
