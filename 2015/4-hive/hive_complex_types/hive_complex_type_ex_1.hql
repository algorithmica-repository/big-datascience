create table test1(id int,
name string,
sal bigint,
sub array<string>,
deductions map<string,int>,
addr struct<city:string,state:string,pin:bigint>
)
row format delimited
fields terminated by ‘,’
collection items terminated by ‘$’
map keys terminated by ‘#’;

load data local inpath ‘/home/cloudera/complex/’ into table test1;

select addr.city, deductions["pf"], sub[0] from test1;