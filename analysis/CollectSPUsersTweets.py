# This MapReduce query extracts all tweets originated by users belong to the subpopulation
# Please make sure you have a list of the subpopulation users you wish to analyze

import os
import sys
import unicodedata
import json
import re


TWITTER_STREAM_FILE_PATH = "../twitter_data/*"
OUTPUT_FOLDER_PATH = "output/SPUsersTweets/1"
OUTPUT_FOLDER_PATH2 = "output/SPUsersTweets/2"
USERS_TO_INCLUDE_FILE_PATH = "../input/users_files/orig/subpopulation_users.txt"


__users_to_include__ = {}


def spark_initiator():
	from pyspark import SparkContext
	from pyspark import SparkConf
	conf = SparkConf().setAppName("CollectSPUsersTweets")
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


def read(line):
	try:
		return json.loads(line)
	except:
		pass


def ignore(s):
	text = unicodedata.normalize('NFKD', unicode(s)).encode('ascii', 'ignore')
	text = text.replace("\n", ' ')
	text = text.replace("\t", ' ')
	text = text.replace("\r", ' ')
	text = text.lower()
	return text


def splitline(line):
	line = re.sub('[(]', "", line)
	line = re.sub('[)]', "", line)
	line = line.replace("'", "").replace(",", "")
	line = line.split(' ')
	return str(line[0])


def flat_mapper(row):

	key_value_list = []

	try:
		if 'actor' and 'body' in row:
			name = ignore(row['actor']['preferredUsername'])
			if name  in __users_to_include__[1]:
				name = str(name)
				tweet = str(ignore(row['body']))
				desc = str(ignore(row['actor']['summary']))
				key_value_list.append(name + '@@@@' + desc + '@@@@' + tweet)
	except:
		pass

	return key_value_list
	

def reducer(accum_param, new_item):
	return accum_param[0], accum_param[1] + new_item[1]


def main():
	sc = spark_initiator()
	rdd_users_to_include = sc.textFile(USERS_TO_INCLUDE_FILE_PATH).filter(lambda line: len(line) > 1)
	load_file(rdd_users_to_include)
	tweet_rdd = sc.textFile(TWITTER_STREAM_FILE_PATH).map(lambda line: read(line))
	result = tweet_rdd.flatMap(lambda row: flat_mapper(row)) \
		.map(lambda edge: (edge, 1)) \
		.reduceByKey(lambda x, y: x + y).repartition(100)
	result.saveAsTextFile(OUTPUT_FOLDER_PATH)
	result2 = sc.textFile(OUTPUT_FOLDER_PATH).filter(lambda x: len(x) > 1).map(lambda line: splitline(line)) \
		.map(lambda line: (line, 1)).reduceByKey(lambda x, y: x + y)
	result2.saveAsTextFile(OUTPUT_FOLDER_PATH2)

	
if __name__ == "__main__":
	main()
