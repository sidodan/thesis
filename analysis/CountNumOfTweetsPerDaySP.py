# This script counts the nuber of tweets per date in the dataset, for different sizes of subpopulations.
# In order to run it, please make sure to have a list of users corresponding to the below, i.e 100,200,500,1000,2000 or similar
import os
import sys
import unicodedata
import json
import re

TWITTER_STREAM_FILE_PATH = "../twitter_data/*"

# Seeds
OUTPUT_FOLDER_PATH = "output/NumOfTweetsPerDay/seeds"
USERS_TO_INCLUDE_FILE_PATH = "../input/users_files/orig/seeds.txt"
# top 200
OUTPUT_FOLDER_PATH1 = "output/NumOfTweetsPerDay/top_200"
USERS_TO_INCLUDE_FILE_PATH1 = "../input/users_files/orig/top_200.txt"
# top 500
OUTPUT_FOLDER_PATH2 = "output/NumOfTweetsPerDay/top_500"
USERS_TO_INCLUDE_FILE_PATH2 = "../input/users_files/orig/top_500.txt"
# top 1000
OUTPUT_FOLDER_PATH3 = "output/NumOfTweetsPerDay/top_1000"
USERS_TO_INCLUDE_FILE_PATH3 = "../input/users_files/orig/top_1000.txt"
# top 2000
OUTPUT_FOLDER_PATH4 = "output/NumOfTweetsPerDay/top_2000"
USERS_TO_INCLUDE_FILE_PATH4 = "../input/users_files/orig/top_2000.txt"


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
				date = timestamp[:10]
				yield (date)
	except:
		pass


def reducer(accum_param, new_item):
	return accum_param[0], accum_param[1] + new_item[1]


def main():
	sc = spark_initiator()
 	sc._jsc.hadoopConfiguration().set('fs.s3.canned.acl','BucketOwnerFullControl')
	rdd_users_to_include = sc.textFile(USERS_TO_INCLUDE_FILE_PATH).filter(lambda line: len(line) > 1)
	load_file(rdd_users_to_include)
	tweet_rdd = sc.textFile(TWITTER_STREAM_FILE_PATH).map(lambda line: read(line))
	result = tweet_rdd.flatMap(lambda row: flat_mapper(row)) \
		.map(lambda date: (date, 1)) \
		.reduceByKey(lambda x, y: x + y).repartition(1)
	result.saveAsTextFile(OUTPUT_FOLDER_PATH)

	rdd_users_to_include1 = sc.textFile(USERS_TO_INCLUDE_FILE_PATH1).filter(lambda line: len(line) > 1)
	load_file(rdd_users_to_include1)
	tweet_rdd1 = sc.textFile(TWITTER_STREAM_FILE_PATH).map(lambda line: read(line))
	result1 = tweet_rdd1.flatMap(lambda row: flat_mapper(row)) \
		.map(lambda date: (date, 1)) \
		.reduceByKey(lambda x, y: x + y).repartition(1)
	result1.saveAsTextFile(OUTPUT_FOLDER_PATH1)

	rdd_users_to_include2 = sc.textFile(USERS_TO_INCLUDE_FILE_PATH2).filter(lambda line: len(line) > 1)
	load_file(rdd_users_to_include2)
	tweet_rdd2 = sc.textFile(TWITTER_STREAM_FILE_PATH).map(lambda line: read(line))
	result2 = tweet_rdd2.flatMap(lambda row: flat_mapper(row)) \
		.map(lambda date: (date, 1)) \
		.reduceByKey(lambda x, y: x + y).repartition(1)
	result2.saveAsTextFile(OUTPUT_FOLDER_PATH2)

	rdd_users_to_include3 = sc.textFile(USERS_TO_INCLUDE_FILE_PATH3).filter(lambda line: len(line) > 1)
	load_file(rdd_users_to_include3)
	tweet_rdd3 = sc.textFile(TWITTER_STREAM_FILE_PATH).map(lambda line: read(line))
	result3 = tweet_rdd3.flatMap(lambda row: flat_mapper(row)) \
		.map(lambda date: (date, 1)) \
		.reduceByKey(lambda x, y: x + y).repartition(1)
	result3.saveAsTextFile(OUTPUT_FOLDER_PATH3)

	rdd_users_to_include4 = sc.textFile(USERS_TO_INCLUDE_FILE_PATH4).filter(lambda line: len(line) > 1)
	load_file(rdd_users_to_include4)
	tweet_rdd4 = sc.textFile(TWITTER_STREAM_FILE_PATH).map(lambda line: read(line))
	result4 = tweet_rdd4.flatMap(lambda row: flat_mapper(row)) \
		.map(lambda date: (date, 1)) \
		.reduceByKey(lambda x, y: x + y).repartition(1)
	result4.saveAsTextFile(OUTPUT_FOLDER_PATH4)


if __name__ == "__main__":
	main()
