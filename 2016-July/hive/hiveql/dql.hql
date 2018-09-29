use test1;

drop table emp;

create table emp(id int, name string, sal int, dept tinyint)
row format delimited
fields terminated by ',';

load data local inpath '/home/cloudera/emp.csv' into table emp;

select count(*)
from emp;

