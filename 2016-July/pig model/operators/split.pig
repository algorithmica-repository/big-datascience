employees = load '/home/cloudera/emp.csv' using PigStorage(',') as(id:int, name:chararray,sal:int,dept:int);

split employees into emp_dept1 if dept<=1, emp_dept2 if dept<=2, emp_others otherwise;

emp1 = union emp_dept1, emp_dept2;

