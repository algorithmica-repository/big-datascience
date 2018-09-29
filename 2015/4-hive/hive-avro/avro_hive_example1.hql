SET hive.exec.compress.output=true;

SET avro.output.codec=snappy;

use default;

DROP TABLE  IF EXISTS tweets;

CREATE EXTERNAL TABLE tweets
    COMMENT "A table backed by Avro data with the Avro schema embedded in the CREATE TABLE statement"
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
    STORED AS
    INPUTFORMAT  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
    LOCATION '/user/cloudera/twitter_avro/'
    TBLPROPERTIES (
        'avro.schema.literal'='{
            "type": "record",
            "name": "Tweet",
            "namespace": "com.alg.twitter.avro",
            "fields": [
                { "name":"username",  "type":"string"},
                { "name":"tweet",     "type":"string"},
                { "name":"timestamp", "type":"long"}
            ]
        }'
    );


DROP TABLE  IF EXISTS tweets_stage;

CREATE TABLE tweets_stage ( username STRING, tweet_count INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

insert overwrite table tweets_stage
select username, count(*)
from tweets
group by username;

select * from tweets_stage order by tweet_count DESC;

