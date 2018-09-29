set io.sort.mb 50

customers = load '$input' using PigStorage(',') as (cust_id:int, fname:chararray, lname:chararray, age:int, profession:chararray);

customers_distinct = distinct customers;

/*store customers_distinct into '$output' using PigStorage('\u0001'); 

customers_sample = sample customers 0.5; */

split customers into customers_25 if age <=25, customers_60 if age <= 60, customers_others otherwise;

dump customers_25;

dump customers_60;

dump customers_others;

/* exec -param input=/home/cloudera/customers.txt -param output=/home/cloudera/dist_out1 operators3.pig */
