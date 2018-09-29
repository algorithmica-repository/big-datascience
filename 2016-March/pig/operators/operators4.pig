employees = load '/home/cloudera/emp.csv' using PigStorage(',') as(id:int, name:chararray,sal:int,dept:int);

emp_dist = distinct employees;

emp_dist_bag = group emp_dist all;

emp_dst_cnt = foreach emp_dist_bag generate COUNT($1);

emp_dist_names = distinct employees.name;
