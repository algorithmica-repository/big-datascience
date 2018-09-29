employees = load '$input1' using PigStorage(',') as(id:int, name:chararray,sal:int,dept:int);
depts = load '$input2' using PigStorage(',') as(dept:int, name:chararray);
/* emp_dept = join employees by dept, depts by dept; */

emp_dept = join employees by dept, depts by dept using 'replicated';
store emp_dept into '$output' using PigStorage(',');

/*  Local mode:
exec -param input1=/home/cloudera/emp.csv -param input2=/home/cloudera/dept.csv -param output=/home/cloudera/join1 innerjoin.pig 

Hadoop mode:
exec -param input1=/user/cloudera/emp.csv -param input2=/user/cloudera/dept.csv -param output=/user/cloudera/join1 innerjoin.pig 
*/
