create table dummy2_report as select name, sal from emp;

create table emp_report(name string, sal int);
insert overwrite table emp_report select name, sal from emp;

insert overwrite directory '/user/cloudera/emp_report' select name, sal from emp;

insert overwrite local directory '/home/cloudera/emp_report' select name, sal from emp;
