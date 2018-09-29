register /usr/lib/pig/piggybank.jar
 
A =  load 'xml_input.xml' using org.apache.pig.piggybank.storage.XMLLoader('book') as (x:chararray);
 
B = foreach A generate flatten(REGEX_EXTRACT_ALL(x,'<book>\\s*<title>(.*)</title>\\s*<author>(.*)</author>\\s*<country>(.*)</country>\\s*<price>(.*)</price>\\s*<year>(.*)</year>\\s*</book>'));
 
dump B;
