use test1;

drop table if exists emp;
drop table if exists dept;

create table emp(id int, name string, sal int, dept int)
row format delimited
fields terminated by ',';

create table dept(dept int, name string)
row format delimited
fields terminated by ',';

load data local inpath '/home/cloudera/emp.csv' into table emp;

load data local inpath '/home/cloudera/dept.csv' into table dept;

select count(*)
from emp e join dept d;

select *
from emp e join dept d on e.dept = d.dept;

select *
from emp e left outer join dept d on e.dept = d.dept;

select *
from emp e left semi join dept d on e.dept = d.dept;

select *
from (select name, dept from emp) e join dept d on e.dept = d.dept;

