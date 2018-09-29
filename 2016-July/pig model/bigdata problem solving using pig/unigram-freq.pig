set io.sort.mb 50

lines = load '$input' using PigStorage('\n') as(line:chararray);
line_words = foreach lines generate TOKENIZE(line) as words;
line_word = foreach line_words generate flatten(words) as word;
word_grouped = group line_word by word;
word_freq = foreach word_grouped generate group as unigram, COUNT(line_word);
store word_freq into '$output' using PigStorage(',');
