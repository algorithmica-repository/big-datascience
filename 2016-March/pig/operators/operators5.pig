employees = load '$input1' using PigStorage(',') as(id:int, name:chararray,sal:int,dept:int);
employees_sample = sample employees 0.5;
dump employees_sample;
