# This MapReduce script generates an hourly word count of words, hashtags and user references (retweets and user mentions).
# Before running the script, please make sure to perform reasonable partitions / run on a subset of the data, as the output might be big.
# Please also make sure to have different subsets of subpopulation users which you like to analyze.
# You can modify the stopwords list, and add or remove words according to your needs.
import os
import sys
import unicodedata
import json
import re

# Data Path
TWITTER_STREAM_FILE_PATH = "../twitter_data/*"
# Stop Words file
EXCLUDED_WORDS_FILE_PATH = "../input/stopwords_300_list.txt"
# Output Paths
OUTPUT_FOLDER_PATH = "output/WordCountByDateAndUserHourly/all_users"
OUTPUT_FOLDER_PATH1 = "output/WordCountByDateAndUserHourly/top_2000"
OUTPUT_FOLDER_PATH2 = "output/WordCountByDateAndUserHourly/seeds"
# SP Users Files
USERS_TO_INCLUDE_FILE_PATH1 = "../input/users_files/orig/top_2000.txt"
USERS_TO_INCLUDE_FILE_PATH2 = "../input/users_files/orig/seeds.txt"


__excluded_words__ = {}
__users_to_include__ = {}


def load_file(rdd):
	for line in rdd.take(rdd.count()):
		key = int(line.split('\t')[0])
		__users_to_include__[key] = set([])
		users_string = line.split('\t')[1]
		users = users_string.split(';')
		for user in users:
			__users_to_include__[key].add(user)


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
	key_value_list = []
	try:
		# body is the the json column holds the tweet itself
		if 'body' in row:
			tweet = str(ignore(row['body']))
			timestamp = str(ignore(row['postedTime']))
			date = timestamp[:13].replace('t','-')
			chunks = re.split(' ', tweet)
			# Print word
			for chunk in chunks:
				match = re.search('^(\w+)(\W.*)?$', chunk)
				if match:
					word = match.group(1)
					if word not in __excluded_words__[1]:
						key_value_list.append(date + ' ' + word)
			# Print Hashtag
			hashtags = re.split('#', tweet)
			hashtags.pop(0)
			for hashtag in hashtags:
				match = re.search('^(\w+)(\W.*)?$', hashtag)
				if match:
					hash = match.group(1)
					if len(hash) > 2:
						key_value_list.append(date + ' ' + '#' + hash)
            		# Print Mention
			mentions = re.split('@', tweet)
			mentions.pop(0)
			for mention in mentions:
				match = re.search('^(\w+)(\W.*)?$', mention)
				if match:
					mention = match.group(1)
					if len(mention) > 2:
						key_value_list.append(date + ' ' + '@' + mention)

	except:
		pass

	return key_value_list


def flat_mapper_user(row):
	key_value_list = []
	try:
		# body is the the json column holds the tweet itself
		if 'body' in row:
			name = ignore(row['actor']['preferredUsername'])
			timestamp = str(ignore(row['postedTime']))
			date = timestamp[:13].replace('t','-')
			if name  in __users_to_include__[1]:
				tweet = str(ignore(row['body']))
				chunks = re.split(' ', tweet)
				# Print word
				for chunk in chunks:
					match = re.search('^(\w+)(\W.*)?$', chunk)
					if match:
						word = match.group(1)
						if word not in __excluded_words__[1]:
							key_value_list.append(date + ' ' + word)
							#yield(word)
				# Print Hashtag
				hashtags = re.split('#', tweet)
				hashtags.pop(0)
				for hashtag in hashtags:
					match = re.search('^(\w+)(\W.*)?$', hashtag)
					if match:
						hash = match.group(1)
						if len(hash) > 2:
							key_value_list.append(date + ' ' + '#' + hash)
							# yield ('#' + hash)
				# Print Mention
				mentions = re.split('@', tweet)
				mentions.pop(0)
				for mention in mentions:
					match = re.search('^(\w+)(\W.*)?$', mention)
					if match:
						mention = match.group(1)
						if len(mention) > 2:
							key_value_list.append(date + ' ' + '@' + mention)
							# yield ('@' + mention)

	except:
		pass

	return key_value_list


# Reducer: simple word count
def reducer(accum_param, new_item):
	return accum_param[0], accum_param[1] + new_item[1]


# Main
def main():
	sc = spark_initiator()
	rdd_excluded_words = sc.textFile(EXCLUDED_WORDS_FILE_PATH).filter(lambda line: len(line) > 1)
	load_excluded_words(rdd_excluded_words)
	tweet_rdd = sc.textFile(TWITTER_STREAM_FILE_PATH).map(lambda line: read(line))
	result = tweet_rdd.flatMap(lambda row: flat_mapper(row)).map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y).repartition(1)
	result.saveAsTextFile(OUTPUT_FOLDER_PATH)

	rdd_users_to_include = sc.textFile(USERS_TO_INCLUDE_FILE_PATH1).filter(lambda line: len(line) > 1)
	load_file(rdd_users_to_include)
	tweet_rdd = sc.textFile(TWITTER_STREAM_FILE_PATH).map(lambda line: read(line))
	result = tweet_rdd.flatMap(lambda row: flat_mapper_user(row)).map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y).repartition(1)
	result.saveAsTextFile(OUTPUT_FOLDER_PATH1)

	rdd_users_to_include2 = sc.textFile(USERS_TO_INCLUDE_FILE_PATH2).filter(lambda line: len(line) > 1)
	load_file(rdd_users_to_include2)
	tweet_rdd = sc.textFile(TWITTER_STREAM_FILE_PATH).map(lambda line: read(line))
	result = tweet_rdd.flatMap(lambda row: flat_mapper_user(row)).map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y).repartition(1)
	result.saveAsTextFile(OUTPUT_FOLDER_PATH2)


if __name__ == "__main__":
	main()
