customers = load '$input' using PigStorage(',') as (cust_id:int, fname:chararray, lname:chararray, age:int, profession:chararray);

customers_grouped = group customers by profession;

customers_avg_age_by_prof = foreach customers_grouped generate group as profession, AVG(customers.age) as avg_age;

dump customers_avg_age_by_prof;

describe customers_avg_age_by_prof;
