employees = load '$input' using PigStorage(',') as (id:int, name:chararray, sal:int, dept:chararray);

emp_filtered = filter employees by sal > 10000;

store emp_filtered into '$output' using PigStorage(',');

