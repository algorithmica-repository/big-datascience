data = [1,2,3,4,5,6,7,8,9,10]
rdd = sc.parallelize(data,4)

squared_rdd = rdd.map(lambda x:x**2)
squared_rdd.collect()

filtered_rdd = rdd.filter(lambda x:x%2==0)
filtered_rdd.collect()

flat_rdd = rdd.flatMap(lambda x:[x,x**3])
flat_rdd.collect()

data = [1,2,2,2,2,3,3,3,3,4,5,6,7,7,7,8,8,8,9,10]
rdd = sc.parallelize(data,4)
distinct_rdd = rdd.distinct()
distinct_rdd.collect()

data = [('Apple','Fruit',200),('Banana','Fruit',24),('Tomato','Vegetable',56),('Potato','Vegetable',103),('Carrot','Vegetable',34)]
rdd = sc.parallelize(data,4)

category_price_rdd = rdd.map(lambda x: (x[1],x[2]))
category_total_price_rdd = category_price_rdd.reduceByKey(lambda x,y:x+y)
category_total_price_rdd.collect()

category_product_rdd = rdd.map(lambda x: (x[1],x[0]))
grouped_products_by_category_rdd = category_product_rdd.groupByKey()
findata = grouped_products_by_category_rdd.collect()

rdd = sc.parallelize([1,2,3,4,5])
rdd.reduce(lambda x,y : x+y)
rdd.take(3)

rdd = sc.parallelize([5,3,12,23])
rdd.takeOrdered(3,lambda s:-1*s) 

rdd = sc.parallelize([(5,23),(3,34),(12,344),(23,29)])
rdd.takeOrdered(3,lambda s:-1*s[1])


