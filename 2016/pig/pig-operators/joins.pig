users = load '/home/cloudera/users.txt' as (userid:chararray, age:int);
pages = load '/home/cloudera/pages.txt' as (url:chararray, userid:chararray);

/*users_pages = join users by userid, pages by userid; */

users_pages = join users by userid left outer, pages by userid;

users_pages = join users by userid right outer, pages by userid;

users_pages = join users by userid full outer, pages by userid;

users_pages = join users by userid, pages by userid;

users_semi_join = foreach users_pages generate users::userid, age;
