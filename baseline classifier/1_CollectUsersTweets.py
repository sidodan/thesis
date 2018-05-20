# This script collects all tweet posted by the users you wish to examine
# Before running this script, please make sure you have the list of users.
import os
import sys
import unicodedata
import json
import re


# Data Path
TWITTER_STREAM_FILE_PATH = "../twitter_data/*"
# Users file
USERS_FILE_PATH = "../input/users_files/orig/users_for_Classification.txt"
# Output Path
OUTPUT_FOLDER_PATH = "RAW_DATA/S3_DATA"

# list of users we wish to extract their data
__users__ = {}


# cloud
def spark_initiator():
	from pyspark import SparkContext
	from pyspark import SparkConf
	conf = SparkConf().setAppName("UsersTweets")
	sc_object = SparkContext(conf=conf)
	return sc_object


# load users list
def load_users(rdd):
	for line in rdd.take(rdd.count()):
		key = int(line.split('\t')[0])
		__users__[key] = set([])
		users_string = line.split('\t')[1]
		users = users_string.split(';')
		for user in users:
			__users__[key].add(user)


# Convert to unicode and remove spaces
def ignore(s):
	text = unicodedata.normalize('NFKD', unicode(s)).encode('ascii', 'ignore')
	text = text.replace('\n', ' ')
	text = text.replace('\t', ' ')
	text = text.replace('\r', ' ')
	text = text.lower()
	return text

	
# read data
def read(line):
	try:
		return json.loads(line)
	except:
		pass

# print tweets for specific users
def flat_mapper(row):
	try:
		if 'body' in row and 'actor' in row:
			name = str(ignore(row['actor']['preferredUsername']))
			if name in __users__[1]:
				tweet = str(ignore(row['body']))
				tweet = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', tweet)
				yield(name + '\t' + tweet)
	except:
		pass


# Main
def main():
	sc = spark_initiator()
	users_to_include = sc.textFile(USERS_FILE_PATH).filter(lambda line: len(line) > 1)
	load_users(users_to_include)
	tweet_rdd = sc.textFile(TWITTER_STREAM_FILE_PATH).map(lambda line: read(line))
	result = tweet_rdd.flatMap(lambda row: flat_mapper(row)).repartition(100)
	result.saveAsTextFile(OUTPUT_FOLDER_PATH)


if __name__ == "__main__":
	main()
