# This MapReduce query extracts all tweets originated by users belong to the subpopulation - slightly different than "CollectSPUsersTweets" 
# Before executing this script, please make sute to have a list of subpopulation users you wish to analyze
import os
import sys
import re

TWITTER_STREAM_FILE_PATH = "../twitter_data/*"
OUTPUT_FOLDER_PATH = "output/SPUsersTweetsLean"
USERS_TO_INCLUDE_FILE_PATH = "../input/users_files/orig/subpopulation_users.txt"

__users_to_include__ = {}


def spark_initiator():
	from pyspark import SparkContext
	from pyspark import SparkConf
	conf = SparkConf().setAppName("CollectSPUsersTweetsLean")
	sc_object = SparkContext(conf=conf)
	return sc_object

	
def load_file(rdd):
	for line in rdd.take(rdd.count()):
		key = int(line.split('\t')[0])
		__users_to_include__[key] = set([])
		users_string = line.split('\t')[1]
		users = users_string.split(';')
		for user in users:
			__users_to_include__[key].add(user)

def mapper(row):
	key_value_list = []
	(name, desc, tweet) = row.split('\t')
	name = str(name)
	if name in __users_to_include__[1]:
		key_value_list.append(str(name) + '@@@@' + str(desc) + '@@@@' + str(tweet))
	return key_value_list

def reducer(accum_param, new_item):
	return accum_param[0], accum_param[1] + new_item[1]

def main():
	sc = spark_initiator()
	rdd_users_to_include = sc.textFile(USERS_TO_INCLUDE_FILE_PATH).filter(lambda line: len(line) > 1)
	load_file(rdd_users_to_include)
	tweet_rdd = sc.textFile(TWITTER_STREAM_FILE_PATH)
	result = tweet_rdd.flatMap(lambda row: mapper(row)) \
		.map(lambda edge: (edge, 1)) \
		.reduceByKey(lambda x, y: x + y).repartition(250)
	result.saveAsTextFile(OUTPUT_FOLDER_PATH)

if __name__ == "__main__":
	main()

