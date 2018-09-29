register /usr/lib/pig/piggybank.jar

tweets = LOAD '/home/cloudera/twitter.json' USING JsonLoader('username:chararray, tweet:chararray, timestamp:int');

store tweets into '/home/cloudera/twitter_avro/' using org.apache.pig.piggybank.storage.avro.AvroStorage(
        'schema', '{
                "type": "record",
                "name": "Tweet",
                "namespace": "com.alg.twitter.avro",
                "fields": [
                    {
                        "name": "username",
                        "type": "string"
                    },
                    {
                        "name": "tweet",
                        "type": "string"
                    },
                    {
                        "name": "timestamp",
                        "type": "long"
                    }
                ],
                "doc:" : "A schema for storing Twitter messages"
        }');


