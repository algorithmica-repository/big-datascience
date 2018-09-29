set io.sort.mb 50

lines = load '$input' using PigStorage('\n') as (line:chararray);
words = foreach lines generate flatten(TOKENIZE(line)) as word;
grouped = group words by word;
counts = foreach grouped generate group, COUNT(words);
store counts into '$output';
