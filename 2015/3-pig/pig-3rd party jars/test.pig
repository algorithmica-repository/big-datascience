register /usr/lib/pig/piggybank.jar
 
define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();
define CustomFormatToISO org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO();
define SubString org.apache.pig.piggybank.evaluation.string.SUBSTRING();
 
set default_parallel 10

set mapred.output.compress true

set avro.output.codec snappy

set io.sort.mb 50
 
enron_messages = load 'sample.txt' using PigStorage('\n') as (
     sql_date:chararray
);

emails = foreach enron_messages generate CustomFormatToISO(sql_date, 'yyyy-MM-dd HH:mm:ss.SSS') as date;

out = foreach emails generate SubString(date, 0,4) as year, SubString(date,5,7) as month, SubString(date, 8,10) as day, SubString(date, 11,13) as hour, SubString(date, 14,16) as mins, SubString(date, 17,19) as secs;

dump out;
