# This script counts the number of specific hashtags twitted per day by all users in the dataset.
# Before executing this script, please make sure you have the hashtags file which you want to count the hashtags from.
import os
import unicodedata
import json
import re
import sys

TWITTER_STREAM_FILE_PATH = "../twitter_data/*"
OUTPUT_FOLDER_PATH = "output/TopHashtagsPerDay/1"
OUTPUT_FOLDER_PATH2 = "output/TopHashtagsPerDay/2"
HASHTAGS_TO_INCLUDE_FILE_PATH = "input/hashtags/hashtags.txt"

__hashtags__ = {}


def spark_initiator():
	from pyspark import SparkContext
	from pyspark import SparkConf
	conf = SparkConf().setAppName("HashtagsByDate")
	sc_object = SparkContext(conf=conf)
	return sc_object


def load_file_hashtags(rdd):
	for line in rdd.take(rdd.count()):
		key = int(line.split('\t')[0])
		__hashtags__[key] = set([])
		hashtags_string = line.split('\t')[1]
		hashtags = hashtags_string.split(';')
		for hashtag in hashtags:
			__hashtags__[key].add(hashtag)


def read(line):
	try:
		return json.loads(line)
	except:
		pass


def ignore(s):
	text = unicodedata.normalize('NFKD', unicode(s)).encode('ascii', 'ignore')
	text = text.replace("\n", " ")
	text = text.replace("\t", " ")
	text = text.replace("\r", " ")
	text = text.lower()
	return text


def splitline(line):
	line = re.sub('[(]', "", line)
	line = re.sub('[)]', "", line)
	line = line.replace("'", "").replace(",", "")
	line = line.split(' ')
	return str(line[1]), int(line[2])


def flat_mapper(row):
	key_value_list = []
	try:
		if 'actor' and 'body' in row:
			name = ignore(row['actor']['preferredUsername'])
			tweet = ignore(row['body'])
			hashtags = re.split('#', tweet)
			hashtags.pop(0)
			for hashtag in hashtags:
				match = re.search('^(\w+)(\W.*)?$', hashtag)
				if match:
					hash = match.group(1)
					if hash in __hashtags__[1]:
						timestamp = str(ignore(row['postedTime']))
						### Hourly
						date = timestamp[:13].replace('t','-')
						### Daily
						# date = timestamp[:10]
						key_value_list.append(hash + ' ' + date)
	except:
		pass

	return key_value_list


def reducer(accum_param, new_item):
	return accum_param[0], accum_param[1] + new_item[1]


def main():
	sc = spark_initiator()
	hashtags_rdd = sc.textFile(HASHTAGS_TO_INCLUDE_FILE_PATH).filter(lambda line: len(line) > 1)
	load_file_hashtags(hashtags_rdd)
	tweet_rdd = sc.textFile(TWITTER_STREAM_FILE_PATH).map(lambda line: read(line))
	result = tweet_rdd.flatMap(lambda row: flat_mapper(row)) \
		.map(lambda edge: (edge, 1)) \
		.reduceByKey(lambda x, y: x + y).repartition(1)
	result.saveAsTextFile(OUTPUT_FOLDER_PATH)

	result2 = sc.textFile(OUTPUT_FOLDER_PATH).filter(lambda x: len(x) > 1).map(lambda line: splitline(line)) \
		.reduceByKey(lambda x, y: x + y).repartition(1)
	result2.saveAsTextFile(OUTPUT_FOLDER_PATH2)


if __name__ == "__main__":
	main()
