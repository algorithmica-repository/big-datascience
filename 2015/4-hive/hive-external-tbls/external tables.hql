create external table if not exists test2(name STRING,sal INT) row format delimited fields terminated by ',' location '/user/cloudera/hive-data';

create external table test3(name STRING,sal INT) row format delimited fields terminated by ',';

load data inpath '/user/cloudera/test3/1.txt' into table test3;
load data local inpath '/home/cloudera/hive1/test.txt' into table test3;
