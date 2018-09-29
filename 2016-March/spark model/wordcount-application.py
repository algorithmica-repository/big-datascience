import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

def normalize(line):
	return line.strip().replace('-','').lower()

lines = sc.textFile(sys.argv[1])
lines_normalized = lines.map(normalize) 
lines_words = lines_normalized.flatMap(lambda x:x.split(' '))
per_word_counts = lines_words.map(lambda x: (x, 1))
word_counts = per_word_counts.reduceByKey(lambda x,y: x + y)
word_counts.saveAsTextFile(sys.argv[2])

#spark-submit wordcount-applicatin.py
