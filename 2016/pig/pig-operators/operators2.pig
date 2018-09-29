set io.sort.mb 50

customers = load '$input' using PigStorage(',') as (cust_id:int, fname:chararray, lname:chararray, age:int, profession:chararray);

customers_grouped = group customers all;

customers_count = foreach customers_grouped generate COUNT(customers);

dump customers_count;


