# This MapReduce script generates a daily word count of words, hashtags and user references (retweets and user mentions).
# Before running the script, please make sure to perform reasonable partitions / run on a subset of the data, as the output might be big.
# You can modify the stopwords list, and add or remove words according to your needs.
import os
import sys
import unicodedata
import json
import re

# Data Path
TWITTER_STREAM_FILE_PATH = "../twitter_data/2015_04*"  # Run on April 2015 Data only
# Output Paths
OUTPUT_FOLDER_PATH = "output/WordCountByDate/2015_04/" # Save a folder for April 2015 Data only
# Stop Words file
EXCLUDED_WORDS_FILE_PATH = "../input/stopwords_300_list.txt"

__excluded_words__ = {}


def load_excluded_words(rdd):
	for line in rdd.take(rdd.count()):
		key = int(line.split('\t')[0])
		__excluded_words__[key] = set([])
		keyphrase_string = line.split('\t')[1]
		keyphrases = keyphrase_string.split(';')
		for keyphrase in keyphrases:
			__excluded_words__[key].add(keyphrase)


def spark_initiator():
	from pyspark import SparkContext
	from pyspark import SparkConf
	conf = SparkConf().setAppName("WordCountByDate")
	sc_object = SparkContext(conf=conf)
	return sc_object


# Convert to unicode and remove spaces
def ignore(s):
	text = unicodedata.normalize('NFKD', unicode(s)).encode('ascii', 'ignore')
	text = text.replace("\n", " ")
	text = text.replace("\t", " ")
	text = text.replace("\r", " ")
	text = text.lower()
	return text


def read(line):
	try:
		return json.loads(line)
	except:
		pass


def flat_mapper(row):
	try:
		# body is the the json column holds the tweet itself
		if 'body' in row:
			tweet = str(ignore(row['body']))
			chunks = re.split(' ', tweet)
			# Print word
			for chunk in chunks:
				match = re.search('^(\w+)(\W.*)?$', chunk)
				if match:
					word = match.group(1)
					if word not in __excluded_words__[1]:
						yield(word)
			# Print Hashtag
			hashtags = re.split('#', tweet)
			hashtags.pop(0)
			for hashtag in hashtags:
				match = re.search('^(\w+)(\W.*)?$', hashtag)
				if match:
					hash = match.group(1)
					if len(hash) > 2:
						yield ('#' + hash)
            		# Print Mention
			mentions = re.split('@', tweet)
			mentions.pop(0)
			for mention in mentions:
				match = re.search('^(\w+)(\W.*)?$', mention)
				if match:
					mention = match.group(1)
					if len(mention) > 2:
						yield ('@' + mention)
	except:
		pass

# Reducer: simple word count
def reducer(accum_param, new_item):
	return accum_param[0], accum_param[1] + new_item[1]


def main():
	sc = spark_initiator()
	rdd_excluded_words = sc.textFile(EXCLUDED_WORDS_FILE_PATH).filter(lambda line: len(line) > 1)
	load_excluded_words(rdd_excluded_words)
	tweet_rdd = sc.textFile(TWITTER_STREAM_FILE_PATH).map(lambda line: read(line))
	result = tweet_rdd.flatMap(lambda row: flat_mapper(row)).map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y).repartition(10)
	result.saveAsTextFile(OUTPUT_FOLDER_PATH)


if __name__ == "__main__":
	main()
