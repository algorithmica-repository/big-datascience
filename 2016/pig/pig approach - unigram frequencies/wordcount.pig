set io.sort.mb 50

lines = load '$input' using PigStorage('\n') as (line:chararray);
line_words = foreach lines generate TOKENIZE(line) as words;
flattend_words = foreach line_words generate flatten(words) as word;
words_grouped = group flattend_words by word;
words_freq = foreach words_grouped generate group, COUNT(flattend_words) as freq;
store words_freq into '$output';

/* how to run the pig scirpt
exec -param input=/home/cloudera/input.txt -param output=/home/cloudera/wc-out2 wordcount.pig
*/

/* how to find the plans
explain -param input=/home/cloudera/input.txt -param output=/home/cloudera/wc-out2 -script wordcount.pig
*/
