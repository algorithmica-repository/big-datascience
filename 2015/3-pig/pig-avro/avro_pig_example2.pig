register /usr/lib/pig/piggybank.jar

set mapred.output.compress true

set avro.output.codec snappy

set io.sort.mb 50

define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();

tweets1 = load '/home/cloudera/twitter_avro/part-m-00000.avro' using AvroStorage();

tweets_grouped = group tweets1 by username;

tweets_count_per_user = foreach tweets_grouped generate group, COUNT(tweets1) as count;

tweets_count_per_user_ord = order tweets_count_per_user by count DESC;

store tweets_count_per_user_ord into '/home/cloudera/twitter_out' using org.apache.pig.piggybank.storage.avro.AvroStorage(
  'schema', '{
  		"name": "CountByUser",
  		"type": "record",
  		"fields": [
    			{"name": "group", "type": "string"},
    			{"name": "count", "type": "int"}
  		]
  }'
);
