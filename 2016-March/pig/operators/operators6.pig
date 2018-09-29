employees = load '$input1' using PigStorage(',') as(id:int, name:chararray,sal:int,dept:int);
depts = load '$input2' using PigStorage(',') as(dept:int, name:chararray);
emp_dept = cogroup employees by dept,depts by dept;
dump emp_dept;
