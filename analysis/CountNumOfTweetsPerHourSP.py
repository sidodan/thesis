# This script counts the number of tweets twitted per hour / per day.
# In case you wish to change the time aggregation, please comment out line 65 for hourly analysis, and line 67 for daily analysis
# Before executing this script, please make sure you have the different users files with different amount of users.
import os
import sys
import unicodedata
import json
import re

TWITTER_STREAM_FILE_PATH = "../twitter_data/*"

OUTPUT_FOLDER_PATH = "output/NumOfTweetsPerDAyAndHour/top_1000"
USERS_TO_INCLUDE_FILE_PATH = "../input/users_files/orig/top_1000.txt"

OUTPUT_FOLDER_PATH1 = "output/NumOfTweetsPerDAyAndHour/top_2000"
USERS_TO_INCLUDE_FILE_PATH1 = "../input/users_files/orig/top_2000.txt"

OUTPUT_FOLDER_PATH2 = "output/NumOfTweetsPerDAyAndHour/seeds"
USERS_TO_INCLUDE_FILE_PATH2 = "../input/users_files/orig/seeds.txt"

__users_to_include__ = {}

def spark_initiator():
	from pyspark import SparkContext
	from pyspark import SparkConf
	conf = SparkConf().setAppName("CountOfTweetsPerDay")
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


def flat_mapper(row):
	try:
		if 'actor' in row and 'body' in row:
			name = ignore(row['actor']['preferredUsername'])
			if name  in __users_to_include__[1]:
				timestamp = str(ignore(row['postedTime']))
				# Hourly
				date = timestamp[:13].replace('t','-')
				### Daily
				# date = timestamp[:10]
				yield (date)
	except:
		pass


def reducer(accum_param, new_item):
	return accum_param[0], accum_param[1] + new_item[1]


def main():
	sc = spark_initiator()
	rdd_users_to_include = sc.textFile(USERS_TO_INCLUDE_FILE_PATH).filter(lambda line: len(line) > 1)
	load_file(rdd_users_to_include)
	tweet_rdd = sc.textFile(TWITTER_STREAM_FILE_PATH).map(lambda line: read(line))
	result = tweet_rdd.flatMap(lambda row: flat_mapper(row)) \
		.map(lambda date: (date, 1)) \
		.reduceByKey(lambda x, y: x + y).repartition(1)
	result.saveAsTextFile(OUTPUT_FOLDER_PATH)

	rdd_users_to_include = sc.textFile(USERS_TO_INCLUDE_FILE_PATH1).filter(lambda line: len(line) > 1)
	load_file(rdd_users_to_include)
	tweet_rdd = sc.textFile(TWITTER_STREAM_FILE_PATH).map(lambda line: read(line))
	result = tweet_rdd.flatMap(lambda row: flat_mapper(row)) \
		.map(lambda date: (date, 1)) \
		.reduceByKey(lambda x, y: x + y).repartition(1)
	result.saveAsTextFile(OUTPUT_FOLDER_PATH1)

	rdd_users_to_include = sc.textFile(USERS_TO_INCLUDE_FILE_PATH2).filter(lambda line: len(line) > 1)
	load_file(rdd_users_to_include)
	tweet_rdd = sc.textFile(TWITTER_STREAM_FILE_PATH).map(lambda line: read(line))
	result = tweet_rdd.flatMap(lambda row: flat_mapper(row)) \
		.map(lambda date: (date, 1)) \
		.reduceByKey(lambda x, y: x + y).repartition(1)
	result.saveAsTextFile(OUTPUT_FOLDER_PATH2)


if __name__ == "__main__":
	main()
