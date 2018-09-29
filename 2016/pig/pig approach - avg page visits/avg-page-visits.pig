set io.sort.mb 50

visits = load '$input' using PigStorage(',') as (user:chararray, url:chararray,time:int);

group_user = group visits by user;

visits_user = foreach group_user generate group, COUNT(visits) as count;

visits_user_all = group visits_user all;
 
avg_vists = foreach visits_user_all generate AVG(visits_user.count);

store avg_vists into '$output';




/*
exec -param input=/home/cloudera/visits.txt -param output=/home/cloudera/page-out avg-page-visits.pig

illustrate -param input=/home/cloudera/visits.txt -param output=/home/cloudera/page-out -script avg-page-visits.pig

explain -param input=/home/cloudera/visits.txt -param output=/home/cloudera/page-out -script avg-page-visits.pig
*/
