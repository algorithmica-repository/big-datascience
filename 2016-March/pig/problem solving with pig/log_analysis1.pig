users = load '$input' using PigStorage(',') as (id:int, url:chararray,age:int,time:int); 

users_filtered = filter users by age>=18 and age<=25;

users_grouped_by_url = group users_filtered by url;

url_freq = foreach users_grouped_by_url generate group as url, COUNT(users_filtered) as freq;

url_freq_sorted = order url_freq by freq DESC;

top_5_urls = limit url_freq_sorted 5;

store top_5_urls into '$output';
