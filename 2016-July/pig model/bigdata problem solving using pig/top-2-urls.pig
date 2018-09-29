set io.sort.mb 50

users = load '$input1' using PigStorage(',') as(userid:int, username:chararray,age:int,pageid:int);
users_20_25 = filter users by age >=25 and age <=30;
page_groups = group users_20_25 by pageid;
page_visits = foreach page_groups generate group as pageid, COUNT(users_20_25) as count;
page_visits_desc = order page_visits by count desc;
top_2_pages = limit page_visits_desc 2;

page_urls = load '$input2' using PigStorage(',') as(pageid:int, url:chararray);
top2_page_urls = join top_2_pages by pageid, page_urls by pageid;
top2_urls = foreach top2_page_urls generate page_urls::pageid,url;
store top2_urls into '$output' using PigStorage(',');

