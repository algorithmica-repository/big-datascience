employees = load '/home/cloudera/emp.csv' using PigStorage(',') as(id:int, name:chararray,sal:int,dept:int);
depts = load '/home/cloudera/dept.csv' using PigStorage(',') as(dept:int, name:chararray);
emp_dept = cogroup employees by dept,depts by dept;
dump emp_dept;
