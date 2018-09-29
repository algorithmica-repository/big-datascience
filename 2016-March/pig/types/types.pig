employees2 = load '/home/cloudera/employee2.csv' using PigStorage('$') as (id:int, name:tuple(fname:chararray, mname:chararray, lname:chararray), sal:int, explist: bag { exp: tuple(dept:chararray, years:int) }, dept:map[chararray]);

dump employees2;
