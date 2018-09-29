register /home/cloudera/piggybank-0.14.0.jar

DEFINE XPath org.apache.pig.piggybank.evaluation.xml.XPath();
 
A =  load 'xml_input.xml' using org.apache.pig.piggybank.storage.XMLLoader('book') as (x:chararray);
 
B = FOREACH A GENERATE XPath(x, 'book/author'), XPath(x, 'book/title'), XPath(x, 'book/country'), XPath(x, 'book/price'), XPath(x, 'book/year');
 
dump B;
