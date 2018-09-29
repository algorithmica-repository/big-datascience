employee = load '/home/cloudera/employee.csv' using PigStorage(',') as(empid:int, ename:chararray,salary:long,dept:int);

employee1 = load '/home/cloudera/employee.csv' using PigStorage(',') as(empid:int, ename,salary,dept);

employee2 = load '/home/cloudera/employee.csv' using PigStorage(',');

employee_filtered = filter employee by salary > 50000;

store employee_filtered into '/home/cloudera/employee-filtered' using PigStorage(',');
