# This Map-Reduce script selects a random data for further analysis
import os
import sys
import unicodedata
import json
import random

TWITTER_STREAM_FILE_PATH =  "../twitter_data/*"
OUTPUT_FOLDER_PATH = "output/RandomTwitterData"


def spark_initiator():
	from pyspark import SparkContext
	from pyspark import SparkConf
	conf = SparkConf().setAppName("RandomTwitterData")
	sc_object = SparkContext(conf=conf)
	return sc_object


def read(line):
	try:
		return json.loads(line)
	except:
		pass


def IgnoreNonAsciiCharactersAndURLs(s):
	text = unicodedata.normalize('NFKD', unicode(s)).encode('ascii', 'ignore')
	text = text.replace('\n', ' ')
	text = text.replace('\t', ' ')
	text = text.replace('\r', ' ')
	text = text.lower()
	return text


def mapper(row):
	key_value_list = []
	try:
		if 'body' in row:
			import random
			r = float(random.random())
			if r < 0.0101:
				name = str(IgnoreNonAsciiCharactersAndURLs(row['actor']['preferredUsername']))
				desc = str(IgnoreNonAsciiCharactersAndURLs(row['actor']['summary']))
				tweet = str(IgnoreNonAsciiCharactersAndURLs(row['body']))
				timestamp = str(row['object']['postedTime'])
				location = str(row['location'])
				key_value_list.append(name + '\t' + desc + '\t' + tweet + '\t' + timestamp + '\t' + location)
	except:
		pass
	return key_value_list


def main():
	sc = spark_initiator()
	tweet_rdd = sc.textFile(TWITTER_STREAM_FILE_PATH).map(lambda line: read(line))
	result = tweet_rdd.flatMap(lambda row: mapper(row))
	result.saveAsTextFile(OUTPUT_FOLDER_PATH)


if __name__ == "__main__":
	main()
