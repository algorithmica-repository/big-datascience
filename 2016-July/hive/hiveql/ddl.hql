use test1;
#default separator: ctrl+A
create table emp(id int, name string, sal int, dept tinyint);

or 
create table test1.emp(id int, name string, sal int, dept tinyint);

create table emp(id int, name string, sal int, dept tinyint)
row format delimited
fields terminated by ',';


load data local inpath '/home/cloudera/emp.csv' into table emp;

drop table emp;
load data inpath '/user/cloudera/emp2.csv' into table emp;


