employees = load '$input' using PigStorage(',') as (id:int, name:chararray, sal:int, dept:chararray);

emp_by_dept = group employees by dept;

emp_by_dept_count = foreach emp_by_dept generate group,COUNT(employees);

dump emp_by_dept_count;

