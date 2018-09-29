employees = load '$input1' using PigStorage(',') as(id:int, name:chararray,sal:int,dept:int);

emp_dept = group employees by dept;

dept_max_sal = foreach emp_dept generate group as dept, MAX(employees.sal) as maxsal;

depts = load '$input2' using PigStorage(',') as(dept:int, name:chararray);

top_sal_per_dept = join depts by dept, dept_max_sal by dept;

result = foreach top_sal_per_dept generate name,maxsal;

store result into '$output' using PigStorage(',');
