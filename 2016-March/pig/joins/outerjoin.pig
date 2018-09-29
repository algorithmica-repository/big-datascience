employees = load '$input1' using PigStorage(',') as(id:int, name:chararray,sal:int,dept:int);
depts = load '$input2' using PigStorage(',') as(dept:int, name:chararray);
emp_dept = join employees by dept FULL OUTER, depts by dept;
dump emp_dept;

/* exec -param input1=/home/cloudera/emp.csv -param input2=/home/cloudera/dept.csv outerjoin.pig */
