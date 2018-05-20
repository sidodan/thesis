# This script counts the number of tweets twitted per user in the entire dataset 
import json
import unicodedata
import os
import sys

TWITTER_STREAM_FILE_PATH = "../twitter_data/*"
OUTPUT_FOLDER_PATH = "output/NumberOfTweetsPerUser"


def spark_initiator():
	from pyspark import SparkContext
	from pyspark import SparkConf
	conf = SparkConf().setAppName("CountNumberofTweetsPerUser")
	sc_object = SparkContext(conf=conf)
	return sc_object


def read(line):
	try:
		return json.loads(line)

	except:
		pass

		
def ignore(s):
	text = unicodedata.normalize('NFKD', unicode(s)).encode('ascii', 'ignore')
	text = text.replace("\n", " ")
	text = text.replace("\t", " ")
	text = text.lower()
	return text


def flat_mapper(row):
	key_value_list = []
	try:
		if 'actor' in row and 'body' in row:
			name = str(ignore(row['actor']['preferredUsername']))
			key_value_list.append(str(name) + ' ' + str('1'))
	except:
		pass

	return key_value_list


def reducer(accum_param, new_item):
	return accum_param[0], accum_param[1] + new_item[1]


def main():
	sc = spark_initiator()
	tweet_rdd = sc.textFile(TWITTER_STREAM_FILE_PATH).map(lambda line: read(line))
	result = tweet_rdd.flatMap(lambda row: flat_mapper(row)).map(lambda edge: (edge, 1)).reduceByKey(lambda x, y: x + y)
	result.saveAsTextFile(OUTPUT_FOLDER_PATH)


if __name__ == "__main__":
	main()
