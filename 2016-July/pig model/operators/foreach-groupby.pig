employee = load '/home/cloudera/employee.csv' using PigStorage(',') as(empid:int, ename:chararray,salary:long,dept:int);

employee_new = foreach employee generate empid,ename,salary,salary+10000 as newsalary;

employee_grouped = group employee by dept;

