set io.sort.mb 50 

users = load '$input1' as (userid:chararray, age:int);
pages = load '$input2' as (url:chararray, userid:chararray);
users_18_25 = filter users by age >=18 and age <=25;
users_pages = join users_18_25 by userid, pages by userid;
users_pages_grouped_by_url = group users_pages by url;
page_visits_freq = foreach users_pages_grouped_by_url generate group as url, COUNT(users_pages) as freq;
page_visits_ordered = order page_visits_freq by freq desc;
top_5_pages = limit page_visits_ordered 5;
store top_5_pages into '$output'; 

/* exec -param input1=/home/cloudera/users.txt -param input2=/home/cloudera/pages.txt -param output=/home/cloudera/top5_out1 top-5-pages.pig */

