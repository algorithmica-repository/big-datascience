#json file with each record in correct format
df = sqlContext.read.load('file:///home/cloudera/sample.json', format='json').show()
#multi-line json file
jsonRDD = sc.textFile('file:///home/cloudera/baby-names.json').map(lambda x: json.loads(x))
jsonRDD.toDf()
jsonRDD = sc.wholeTextFiles('file:///home/cloudera/baby-names.json').map(lambda x: x[1])
df = sqlContext.read.load(jsonRDD,format='json')
