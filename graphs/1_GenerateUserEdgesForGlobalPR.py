# This script generates a list of user to user edges for a global graph.
# Edges are generated based on "retweet" (RT @user) or mentions (@user)
# Pleas make sure to have a list of users to exclude from the analysis. Another option is to edit "users_to_exclude" file:
# simply add "1	u1;" with a tab separator, and automatically all users will be included in the analysis 
import os
import sys
import unicodedata
import json
import re

TWITTER_STREAM_FILE_PATH = "../twitter_data/*"

EXCLUDED_USERS_FILE_PATH = "../input/users_files/orig/users_to_exclude.txt"

OUTPUT_FOLDER_PATH = "data/global/UserEdges"

__excluded_users__ = {}

# Excluded users are users we are not interested in. For example, very popular users, having large amount of in-degrees or out-degrees 
def load_excluded_users(rdd):
	for line in rdd.take(rdd.count()):
		key = int(line.split('\t')[0])
		__excluded_users__[key] = set([])
		keyphrase_string = line.split('\t')[1]
		keyphrases = keyphrase_string.split(';')
		for keyphrase in keyphrases:
			__excluded_users__[key].add(keyphrase)


def spark_initiator():
	from pyspark import SparkContext
	from pyspark import SparkConf
	conf = SparkConf().setAppName("GenerateUserEdgesForGlobalPR")
	sc_object = SparkContext(conf=conf)
	return sc_object


def ignore(s):
	text = unicodedata.normalize('NFKD', unicode(s)).encode('ascii', 'ignore')
	text = text.replace("\n", " ")
	text = text.replace("\t", " ")
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
		if 'actor' in row:
			name = str(ignore(row['actor']['preferredUsername']))
			if name not in __excluded_users__[1]:
				if 'body' in row:
					tweet = str(ignore(row['body']))
					# search for metion or retweet simbol "@" 
					chunks = re.split('@', tweet)
					chunks.pop(0)
					for chunk in chunks:
						match = re.search('^(\w+)(\W.*)?$', chunk)
						if match:
							mention = match.group(1)
							if len(mention) > 2:
								if name != mention and mention not in __excluded_users__[1]:
									key_value_list.append(name + ' ' + mention)
	except:
		pass

	return key_value_list


def reducer(accum_param, new_item):
	return accum_param[0], accum_param[1] + new_item[1]


def main():
	sc = spark_initiator()
	rdd_excluded_users = sc.textFile(EXCLUDED_USERS_FILE_PATH).filter(lambda line: len(line) > 1)
	load_excluded_users(rdd_excluded_users)
	tweet_rdd = sc.textFile(TWITTER_STREAM_FILE_PATH).map(lambda line: read(line))
	# Repartition is determined based on graph size
	result = tweet_rdd.flatMap(lambda row: flat_mapper(row)).map(lambda edge: (edge, 1)).reduceByKey(lambda x, y: x + y).repartition(1000)
	result.saveAsTextFile(OUTPUT_FOLDER_PATH)
	

if __name__ == "__main__":
	main()
	