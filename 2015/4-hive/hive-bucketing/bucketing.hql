set hive.exec.dynamic.partition.mode=nonstrict;
set hive.enforce.bucketing = true;

drop table if exists sample1; 

create table sample1(name string, sal int) partitioned by (id int)
clustered by(name) into 5 buckets
row format delimited 
fields terminated by ',';

load data local inpath '/home/cloudera/hive1/1.txt' INTO TABLE sample1 PARTITION (id=1);

load data local inpath '/home/cloudera/hive1/2.txt' INTO TABLE sample1 PARTITION (id=2);

load data local inpath '/home/cloudera/hive1/3.txt' INTO TABLE sample1 PARTITION (id=3);

select * from sample1 tablesample(bucket 1 out of 5 on name);


 
