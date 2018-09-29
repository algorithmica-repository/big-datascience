employees = load '/home/cloudera/emp.csv' using PigStorage(',') as(id:int, name:chararray,sal:int,dept:int);

emp_sample = sample employees 0.8;

dump emp_sample;
