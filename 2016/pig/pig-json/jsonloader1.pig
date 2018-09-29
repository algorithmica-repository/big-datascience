register /usr/lib/pig/piggybank.jar

set io.sort.mb 50

define JsonLoader org.apache.pig.builtin.JsonLoader();

test = load '$input' 
    using JsonLoader('food:chararray, person:chararray, amount:int');

dump test;



