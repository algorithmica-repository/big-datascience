visits = LOAD '$input' USING PigStorage('\t') as (user:chararray,url:chararray,time:int);

user_visits = group visits by user;

user_counts = foreach user_visits generate group as user, COUNT(visits) as numvisits;

all_counts = group user_counts all;

avg_count = foreach all_counts generate AVG(user_counts.numvisits);

dump avg_count;

