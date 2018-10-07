#lines = sc.textFile("/user/cloudera/bible.txt") 

def normalize(line):
	return line.strip().replace('-','').lower()

lines = sc.parallelize(['Spark is fun to try and its very fun to learn-','but you have to know how.','Spark uses Cache'])

lines_normalized = lines.map(normalize) 
lines_normalized.collect()
lines_words = lines_normalized.flatMap(lambda line:line.split(' '))
lines_words.collect()
per_word_counts = lines_words.map(lambda x: (x, 1))
per_word_counts.collect()
word_counts = per_word_counts.reduceByKey(lambda x,y: x + y,2)
word_counts.collect()
word_counts_rev = word_counts.map(lambda x:(x[1],x[0]))
word_counts_rev.collect()
word_counts_by_freq =	word_counts_rev.sortByKey(ascending=False)
word_counts_by_freq.collect()
wordcounts.take(10)
