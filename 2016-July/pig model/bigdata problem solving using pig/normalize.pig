set io.sort.mb 50
register /usr/lib/pig/piggybank.jar
define tolower org.apache.pig.piggybank.evaluation.string.LOWER();

lines = load '$--------------------------input' using PigStorage('\n') as(line:chararray);
lines_normalized = foreach lines generate tolower(line);
store lines_normalized into '$output';
