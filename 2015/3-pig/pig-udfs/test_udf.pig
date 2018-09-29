register /home/cloudera/pig-udf-0.0.1.jar

define UPPER com.alg.pig.udfs.UPPER();

emp = load 'emp.txt' using PigStorage(',') as(empid:chararray, ename:chararray, sal:int, dept:int);
names_upper = foreach emp generate UPPER(ename);
dump names_upper;
