%default input '/home/cloudera/default.txt'
%default output '/home/cloudera/out'

a = LOAD '$input' using PigStorage('|') 
    AS (employee_id:int, email:chararray, name:tuple(first_name:chararray, middle_name:chararray, last_name:chararray), 
	project_list:bag{project: tuple(project_name:chararray, duration:int)}, skills:map[chararray]) ;
DESCRIBE a ;
DUMP a ;

b = FOREACH a GENERATE employee_id, email, name.first_name, project_list, skills#'programming' ;
DESCRIBE b ;

store b into '$output';

#pig -param_file /home/cloudera/parameters.txt -f /home/cloudera/pig_complex_type_ex_2.pig

#pig -param "input=/user/cloudera/complex.txt" -param "output=/home/cloudera/out" -f  /home/cloudera/pig_complex_type_ex_2.pig
