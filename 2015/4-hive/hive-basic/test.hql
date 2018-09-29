drop table if exists test1;

create table if not exists test1(name STRING,sal INT) row format delimited fields terminated by ',';

load data local inpath '/home/cloudera/hive1/test.txt' into table test1;

select sal, COUNT(*)
from test1
group by sal;

-- create table if not exists test2(name STRING);

--insert overwrite table test2 select name from test1;

-- select * from test2;
