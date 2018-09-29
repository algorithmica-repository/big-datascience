sh rm -r emp_out 

fs mkdir /home/cloudera/tmp 

set io.sort.mb 50

set debug on

set job.name test_script

emp = load 'emp.txt' using PigStorage(',') as(empid:chararray, ename:chararray, sal:int, dept:int);

emp_ordered_sal = order emp by sal desc;
emp_top_2 = limit emp_ordered_sal 2;
dump emp_top_2;

store emp_ordered_sal into '$output' using PigStorage('$');


pruned = foreach emp generate empid, ename, sal+5000 as new_sal; 
under_20000 = filter pruned by new_sal < 20000;
dump under_20000;

emp_by_dept = group emp by (dept,sal);
describe emp_by_dept; 
dump emp_by_dept; 

emp_all = group emp ALL;
emp_count = foreach emp_all generate SUM(emp.sal);
dump emp_count;

emp_by_dept = group emp by dept;
avg_sal_dept = foreach emp_by_dept generate group, AVG(emp.sal);
dump avg_sal_dept; 

depts = foreach emp generate dept;
dept_distinct =  distinct depts;
dump dept_distinct; 

emp_by_dept = group emp by dept;
emp1 = foreach emp_by_dept generate group, flatten(emp);

dump emp1; 

emp = load '$input1' using PigStorage(',') as(empid:chararray, ename:chararray, sal:int, dept:chararray);

dept = load '$input2' using PigStorage(',') as(deptid:chararray, dname:chararray);

emp_dept = cogroup emp by dept, dept by deptid;

describe emp_dept;

dump emp_dept;

emp = load 'emp.txt' using PigStorage(',') as(empid:chararray, ename:chararray, sal:int, dept:int);

split emp into d1 if dept==1, d2 if dept==2, d3 if dept==3, d4 otherwise;

