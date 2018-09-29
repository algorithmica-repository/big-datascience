a = load 'complex.txt' using PigStorage('|') 
    as (employee_id:int, email:chararray, name:tuple(first_name:chararray, middle_name:chararray, last_name:chararray), 
	project_list:bag{project: tuple(project_name:chararray, duration:int)}, skills:map[chararray]) ;
describe a ;
dump a ;

