employees = load '/home/cloudera/employee2.csv' using PigStorage('$') as (id:int, name:tuple(fname:chararray, mname:chararray, lname:chararray), sal:int, exp_list: bag { exp: tuple(dept:chararray, years:int) }, dept:map[chararray]);

emp_filtered = filter employees by sal > 10000;

dump emp_filtered;

#emp_names = foreach employees2 generate id+20, name;

#dump emp_names;
