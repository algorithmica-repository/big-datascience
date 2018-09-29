register /usr/lib/pig/piggybank.jar
register /usr/lib/pig/lib/avro-*.jar

set mapred.output.compress true

set io.sort.mb 50

export PIG_HEAPSIZE=2096 

set avro.output.codec snappy

records = load '$input' USING PigStorage(',');
dump records;

