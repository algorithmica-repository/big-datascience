from pyspark.sql import Row
from pyspark.sql.types import *

#Creating data frame
Employee = Row('fname', 'lname', 'age')
data_rows = [
    Employee('John', 'Smith', 47),
    Employee('Jane', 'Smith', 22),
    Employee('Frank', 'Jones', 28)
]

data = [Row('John', 'Smith', 47), Row('Jane', 'Smith', 22), Row('Frank', 'Jones', 28)]
schema = ['fname', 'lname', 'age']

# create a DataFrame from an RDD of Rows
data_rdd = sc.parallelize(data)
df = sqlContext.createDataFrame(data_rdd, schema)

#Retrieving contents of data frame
df.printSchema()
df.show()
df.first()
df.count()

#Adding columns
df = df.withColumn('C', F.lit(0))
df.show()
df.withColumn('D', df.A * 2).show()
df.withColumn('E', df.B > 0).show()

#Subsetting columns
df.select('A','B').show()

#Filtering rows
df.filter(df.B > 5).show()

#Grouped aggregations
df.groupBy("A").avg("B").show()
df.groupBy("A").agg(F.avg("B"), F.min("B"), F.max("B")).show()
df.groupBy("A").agg(
	F.first("B").alias("my first"),
	F.last("B").alias("my last"),
	F.sum("B").alias("my everything")
).show()

