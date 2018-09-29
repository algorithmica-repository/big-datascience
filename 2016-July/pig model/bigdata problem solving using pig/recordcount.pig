set io.sort.mb 50

emp = load '$input' using PigStorage(',') as(empid:int, ename:chararray,salary:int,deptno:int);
emp_all = group emp all;
emp_count = foreach emp_all generate COUNT(emp) as count;
store emp_count into '$output';

