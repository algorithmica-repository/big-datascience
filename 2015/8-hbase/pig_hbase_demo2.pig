raw_data = load 'hbase://customers1' using org.apache.pig.backend.hadoop.hbase.HBaseStorage(
'customers_data:firstname 
 customers_data:lastname 
 customers_data:age 
 customers_data:profession'
) 
as (custno:chararray, firstname:chararray, lastname:chararray, age:int, profession:chararray);

dump raw_data;
