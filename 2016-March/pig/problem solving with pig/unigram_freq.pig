lines = load '$input' using PigStorage('\n') as(line:chararray);
line_words = foreach lines generate TOKENIZE(line) as words;
words = foreach line_words generate flatten(words) as word;
words_grouped = group words by word;
words_freq = foreach words_grouped generate group, COUNT(words) as freq;
store words_freq into '$output';
