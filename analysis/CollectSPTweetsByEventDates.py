# This Map-Reduce Script extracts tweets originated by users from the subpopulation, which occurred during specific dates 
# Before executing this script, please make sure to have a list of users to analyze, as well as a list of dates you wish to include.
import os
import sys
import unicodedata
import json

# Data Path
TWITTER_STREAM_FILE_PATH = "../twitter_data/*"
# Output Path
OUTPUT_FOLDER_PATH = "output/SPTweetsByEventDates"
# Sub-population File Path
USERS_TO_INCLUDE_FILE_PATH = "../input/users_files/orig/subpopulation_users.txt"
# Dates File Path
DATES_TO_INCLUDE_FILE_PATH = "../input/dates/*.txt"

__users_to_include__ = {}
__dates_to_include__ = {}


def spark_initiator():
	from pyspark import SparkContext
	from pyspark import SparkConf
	conf = SparkConf().setAppName("ExtractEventsData")
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


def load_file_dates(rdd):
	for line in rdd.take(rdd.count()):
		key = int(line.split('\t')[0])
		__dates_to_include__[key] = set([])
		dates_string = line.split('\t')[1]
		dates = dates_string.split(';')
		for date in dates:
			__dates_to_include__[key].add(date)


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
		if 'actor' and 'body' in row:
			name = str(ignore(row['actor']['preferredUsername']))
			if name in __users_to_include__[1]:
				tweet = str(ignore(row['body']))
				timestamp = str(ignore(row['postedTime']))
				date = timestamp[:10]
				if date in __dates_to_include__[1]:
					yield (name + '\t' + tweet + '\t' + date + '\t' + timestamp)
	except:
		pass


def main():
	sc = spark_initiator()
	rdd_users_to_include = sc.textFile(USERS_TO_INCLUDE_FILE_PATH).filter(lambda line: len(line) > 1)
	load_file(rdd_users_to_include)
	rdd_dates_to_include = sc.textFile(DATES_TO_INCLUDE_FILE_PATH).filter(lambda line: len(line) > 1)
	load_file_dates(rdd_dates_to_include)
	tweet_rdd = sc.textFile(TWITTER_STREAM_FILE_PATH).map(lambda line: read(line))
	# The partition parameter may vary according to the expected data volume
	result = tweet_rdd.flatMap(lambda row: flat_mapper(row)).repartition(10)
	result.saveAsTextFile(OUTPUT_FOLDER_PATH)


if __name__ == "__main__":
	main()
