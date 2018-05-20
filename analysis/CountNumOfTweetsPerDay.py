# This script counts the number of tweets twitted each day in the entire dataset
import os
import sys
import unicodedata
import json
import re

TWITTER_STREAM_FILE_PATH = "../twitter_data/*"
OUTPUT_FOLDER_PATH = "output/CountOfTweetsPerDay"


def spark_initiator():
	from pyspark import SparkContext
	from pyspark import SparkConf
	conf = SparkConf().setAppName("CountOfTweetsPerDay")
	sc_object = SparkContext(conf=conf)
	return sc_object


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
			timestamp = str(ignore(row['postedTime']))
			date = timestamp[:10]
			yield (date)
	except:
		pass


def reducer(accum_param, new_item):
	return accum_param[0], accum_param[1] + new_item[1]


def main():
	sc = spark_initiator()
	tweet_rdd = sc.textFile(TWITTER_STREAM_FILE_PATH).map(lambda line: read(line))
	result = tweet_rdd.flatMap(lambda row: flat_mapper(row)) \
		.map(lambda date: (date, 1)) \
		.reduceByKey(lambda x, y: x + y).repartition(1)
	result.saveAsTextFile(OUTPUT_FOLDER_PATH)


if __name__ == "__main__":
	main()
