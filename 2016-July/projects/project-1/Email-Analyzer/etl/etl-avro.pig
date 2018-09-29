register /usr/lib/pig/piggybank.jar
 
define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();
define CustomFormatToISO org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO();
define SubString org.apache.pig.piggybank.evaluation.string.SUBSTRING();
 
set default_parallel 10

set mapred.output.compress true

set avro.output.codec snappy

set io.sort.mb 50
 
enron_messages = load '/user/cloudera/enron/message/part-*' using PigStorage(',') as (
     message_id:int,
     from_address:chararray,
     sql_date:chararray,
     message_id_internal:chararray,
     subject:chararray,
     body:chararray,
     folder:chararray
);
 
enron_recipients = load '/user/cloudera/enron/recipientinfo/part-*' using PigStorage(',') as (
    reference_id:int,
    message_id:int,
    recip_type:chararray,
    address:chararray,
    dater:chararray
);
 
split enron_recipients into tos IF recip_type=='TO', ccs IF recip_type=='CC', bccs IF recip_type=='BCC';
 
headers = cogroup tos by message_id, ccs by message_id, bccs by message_id;
with_headers = join headers by group, enron_messages by message_id;
emails = foreach with_headers generate enron_messages::message_id as message_id,
                                  CustomFormatToISO(enron_messages::sql_date, 'yyyy-MM-dd HH:mm:ss.SSS') as date,
                                  enron_messages::from_address as from_address,
                                  enron_messages::subject as subject,
                                  enron_messages::body as body,
                                  headers::tos.address as tos,
                                  headers::ccs.address as ccs,
                                  headers::bccs.address as bccs;
 
out = limit emails 10;

dump out;
