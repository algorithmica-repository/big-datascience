raw_data = load 'hdfs:/user/cloudera/customers' using PigStorage(',') as (
           custno:chararray, firstname:chararray, lastname:chararray, age:int, profession:chararray);

store raw_data into 'hbase://customers1' using org.apache.pig.backend.hadoop.hbase.HBaseStorage(
'customers_data:firstname 
 customers_data:lastname 
 customers_data:age 
 customers_data:profession'
);
