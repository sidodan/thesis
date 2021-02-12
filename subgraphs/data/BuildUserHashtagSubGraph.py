import re
import os
import sys

INPUT_FOLDER_PATH = "../graphs/data/hashtags"
OUTPUT_FOLDER_PATH = "../subgraphs/data/hashtags"
SEED_FILE_PATH = "../input/hashtags/hashtags.txt"

__seeds__ = {}


def spark_initiator():
	from pyspark import SparkContext
	from pyspark import SparkConf
	conf = SparkConf().setAppName("GenerateSubGraph")
	sc_object = SparkContext(conf=conf)
	return sc_object


def load_seed(rdd):
	for line in rdd.take(rdd.count()):
		key = int(line.split('\t')[0])
		__seeds__[key] = set([])
		seeds_list = line.split('\t')[1]
		seeds = seeds_list.split(';')
		for seed in seeds:
			__seeds__[key].add(seed)


def edge(node1, node2):
	if node1 < node2:
		return node1 + node2
	return node2 + node1


def flatmapper_1(line):
	seeds_found = set([])
	line = line.strip()
	line = line.lower()
	match = re.search(r'\(\(\'([^\']+)\', \{([^\}]+)\}\), \d+\)', line)
	if not match:
		return
	center = match.group(1)
	neighbor_list = match.group(2)
	neighbors = re.findall(r'\'([^\']+)\':', neighbor_list)

	if len(neighbors) > 10000:
		return
	yield (center, neighbors)
	for neighbor in neighbors:
		if neighbor in __seeds__[1]:
			seeds_found.add(neighbor)
	if len(seeds_found) > 0:
		for neighbor in neighbors:
			for seed in seeds_found:
				if neighbor != seed:
					yield (neighbor, [(center, seed)])


def flatmapper_2(tup):
	tuples = set([])
	neighbors = set([])
	(node, values) = tup
	seen = set([node])
	for value in values:
		if type(value) == tuple:
			tuples.add(value)
		else:
			neighbors.add(value)
	if node in __seeds__[1]:
		for neighbor in neighbors:
			yield (node, set([edge(node + '@@0@@', neighbor + '@@1@@')]))
	for (neighbor, seed) in tuples:
		yield (seed, set([edge(node + '@@2@@', neighbor + '@@1@@')]))
		seen.add(neighbor)
		seen.add(seed)
	if len(tuples) > 0:
		for neighbor in neighbors:
			if neighbor not in seen:
				yield (seed, set([edge(node + '@@2@@', neighbor + '@@?@@')]))


def reducer_1(aggregate, value):
	return aggregate + value


def reducer_2(aggregate, value):
	return aggregate.union(value)


def postprocessing(tup):
	nodes = set([])
	edge_set = set([])
	vector_set = set([])
	seen = set([])
	(seed, edges) = tup
	for edge in edges:
		edge = edge.split('@@')
		if edge[1].isdigit():
			nodes.add(edge[0])
		if edge[3].isdigit():
			nodes.add(edge[2])
	for edge in edges:
		edge = edge.split('@@')
		if edge[0] in nodes and edge[2] in nodes:
			edge_set.add(str(edge[0]) + '@' + str(edge[2]))
			if edge[0] not in seen:
				if edge[0] == seed:
					vector_set.add(str(edge[0]) + ':' + '1')
                    			seen.add(edge[0])
				else:
					vector_set.add(str(edge[0]) + ':' + '0.0000001')
					seen.add(edge[0])

			if edge[2] not in seen:
				if edge[2] == seed:
					vector_set.add(str(edge[2]) + ':' + '1')
					seen.add(edge[2])
				else:
					vector_set.add(str(edge[2]) + ':' + '0.0000001')
					seen.add(edge[2])
	return seed + '\t' + ','.join(edge_set) + '\t' + ','.join(vector_set)


def main():
	sc = spark_initiator()
	sc._jsc.hadoopConfiguration().set('fs.s3.canned.acl', 'BucketOwnerFullControl')
	rdd_seeds = sc.textFile(SEED_FILE_PATH)
	load_seed(rdd_seeds)
	graph_file = sc.textFile(INPUT_FOLDER_PATH)
	result = graph_file.flatMap(flatmapper_1) \
		.reduceByKey(reducer_1) \
		.flatMap(flatmapper_2) \
		.reduceByKey(reducer_2) \
		.map(postprocessing)
	result.saveAsTextFile(OUTPUT_FOLDER_PATH)

if __name__ == "__main__":
	main()
