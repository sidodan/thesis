# This query extracts tweets originated by all users in the dataset, on a specific set of dates. 
# Before executing this script, please make sure to have a file with list of dates you wish to examine
import os
import sys
import unicodedata
import json

# Data Path
TWITTER_STREAM_FILE_PATH = "../input/twitter_data/*"
# Output Path
OUTPUT_FOLDER_PATH = "output/SPTweetsEventDatesAllUsers"
# Dates File Path
DATES_TO_INCLUDE_FILE_PATH = "../input/dates/*"

__dates_to_include__ = {}


def spark_initiator():
	from pyspark import SparkContext
	from pyspark import SparkConf
	conf = SparkConf().setAppName("ExtractEventsData")
	sc_object = SparkContext(conf=conf)
	return sc_object


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
			tweet = str(ignore(row['body']))
			timestamp = str(ignore(row['postedTime']))
			date = timestamp[:10]
			if date in __dates_to_include__[1]:
				yield (name + '\t' + tweet + '\t' + date + '\t' + timestamp)

	except:
		pass


def main():
	sc = spark_initiator()
	rdd_dates_to_include = sc.textFile(DATES_TO_INCLUDE_FILE_PATH).filter(lambda line: len(line) > 1)
	load_file_dates(rdd_dates_to_include)
	tweet_rdd = sc.textFile(TWITTER_STREAM_FILE_PATH).map(lambda line: read(line))
	result = tweet_rdd.flatMap(lambda row: flat_mapper(row))
	result.saveAsTextFile(OUTPUT_FOLDER_PATH)


if __name__ == "__main__":
	main()
