create table sample(name STRING,sal INT) partitioned by(id INT) row format delimited fields terminated by ',';

1) Loading segregated data into partitioned table
--------------------------------------------------
load data local inpath '/home/cloudera/hive1/1.txt' INTO TABLE sample PARTITION (id=1);

load data local inpath '/home/cloudera/hive1/2.txt' INTO TABLE sample PARTITION (id=2);

alter table sample add partition(id=3) location '/user/cloudera/3';

2) Loading non-segregated data into partitioned table
-----------------------------------------------------
create external table sample_staging(name STRING,sal INT, id INT) row format delimited fields terminated by ',' location '/user/cloudera/staging';

insert overwrite table sample partition(id) select name, sal, id from sample_staging;
      or

insert overwrite table sample partition(id=1) select name,sal from sample_staging where id=1;
insert overwrite table sample partition(id=2) select name,sal from sample_staging where id=2;
insert overwrite table sample partition(id=3) select name,sal from sample_staging where id=3;
