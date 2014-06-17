// QUERIES FOR THE NETFLIX DATA SET 
create external table movietitles (mid int, yearofrelease int, title string) row format delimited fields terminated by ',' stored as textfile location 's3n://spring-2014-ds/movie_dataset/movie_titles';

create external table movieratings (mid int, customerid int, rating int, date string) row format delimited fields terminated by ',' stored as textfile location 's3n://spring-2014-ds/movie_dataset/movie_ratings';

create table currentavgratings (mid int, title string, avgrat float);


insert into table currentavgratings select mr.mid, mt.title, mr.avgrat from movietitles mt join (select mid, avg(rating) as avgrat from movieratings mr group by mid) mr on (mr.mid = mt.mid);

insert overwrite directory 's3://netflix-hive/output/highlow' select * from currentavgratings order by avgrat desc limit 10;

insert overwrite directory 's3://netflix-hive/output/highlow1' select * from currentavgratings order by avgrat limit 10;

insert overwrite directory 's3://netflix-hive/output/customerdatewise' select mr.customerid, count(mr.mid), avg(mr.rating), mr.date from movieratings mr join (select customerid, count(rating) as countrating from movieratings group by customerid order by countrating desc limit 10) cus on mr.customerid = cus.customerid group by mr.customerid, mr.date;

insert overwrite directory 's3://netflix-hive/output/datewise' select mt.mid, mt.title, mr.date, mr.avgrat from movietitles mt join (select mid, date, avg(rating) as avgrat from movieratings group by mid, date) mr on (mt.mid = mr.mid) join (select * from currentavgratings order by avgrat desc limit 10) ca on ca.mid = mt.mid ;

insert overwrite directory 's3://netflix-hive/output/yearwise' select mt.mid, mt.title, mr.year, mr.avgrat from movietitles mt join (select mid, year(date) as year, avg(rating) as avgrat from movieratings group by mid, year(date)) mr on (mt.mid = mr.mid) join (select * from currentavgratings order by avgrat desc limit 10) ca on ca.mid = mt.mid ;

insert overwrite directory 's3://netflix-hive/output/yearwisemoviesreleased' select yearofrelease, count(mid) from movietitles group by yearofrelease;

create table releaseyearratings (mid int, avgrating float, noofCustomers int, year int);

insert into table releaseyearratings select tab.mid, avg(tab.rating) as rat, count(1), tab.year as yearval from (select mr.mid, mt.title, mr.rating, mt.yearofrelease as year from movieratings mr join movietitles mt on mr.mid = mt.mid where year(mr.date) = mt.yearofrelease) tab group by tab.year, tab.mid order by yearval, rat desc;

insert overwrite directory 's3://netflix-hive/output/topmoviereleasedyearwise' select tab.mid, avg(tab.rating) as rat, count(1), tab.year as yearval from (select mr.mid, mt.title, mr.rating, mt.yearofrelease as year from movieratings mr join movietitles mt on mr.mid = mt.mid where year(mr.date) = mt.yearofrelease) tab group by tab.year, tab.mid order by yearval, rat desc;

create table ratingdeviation (mid int, avgrat float, customerid int, rating int, deviation int);
insert into table ratingdeviation select x.mid, x.avgrat, mr.customerid, mr.rating, abs(x.avgrat - mr.rating) from currentavgratings x join movieratings mr on x.mid = mr.mid;

create table maxdeviation (mid int, maxdeviation float);
insert into table maxdeviation select mid, max(deviation) as negdiff from ratingdeviation  group by mid;

insert overwrite directory 's3://netflix-hive/output/customerdeviation' select z.customerid, count(z.customerid) as frequency from maxdeviation md join ratingdeviation z on md.mid = z.mid and md.maxdeviation = z.deviation group by z.customerid order by frequency desc limit 100;

create table ratingfrequency (mid int, one int, two int, three int, four int, five int);

insert into table ratingfrequency select r1.mid, r1.countrating, r2.countrating, r3.countrating, r4.countrating, r5.countrating from (select mid, count(mid) as countrating from movieratings where rating = 1 group by mid, rating) r1 join (select mid, count(mid) as countrating from movieratings where rating = 2 group by mid, rating) r2 on r1.mid = r2.mid  join (select mid, count(mid) as countrating from movieratings where rating = 3 group by mid, rating) r3 on r1.mid = r3.mid join (select mid, count(mid) as countrating from movieratings where rating = 4 group by mid, rating) r4 on r1.mid = r4.mid join (select mid, count(mid) as countrating from movieratings where rating = 5 group by mid, rating) r5 on r1.mid = r5.mid;

insert overwrite directory 's3://netflix-hive/output/ratingfrequency' select * from ratingfrequency rf join (select * from currentavgratings order by avgrat desc limit 10) ca on rf.mid = ca.mid;

insert overwrite directory 's3://netflix-hive/output/ratio1' select title, one, five from ratingfrequency join movietitles on ratingfrequency.mid = movietitles.mid order by one/five desc limit 10;

insert overwrite directory 's3://netflix-hive/output/ratio2' select title, one, five from ratingfrequency join movietitles on ratingfrequency.mid = movietitles.mid order by one/five limit 10;

create table customerratingfrequency (customerid int, one int, two int, three int, four int, five int);

insert into table customerratingfrequency select r1.customerid, r1.countrating, r2.countrating, r3.countrating, r4.countrating, r5.countrating from (select customerid, count(customerid) as countrating from movieratings where rating = 1 group by customerid, rating) r1 join (select customerid, count(customerid) as countrating from movieratings where rating = 2 group by customerid, rating) r2 on r1.customerid = r2.customerid  join (select customerid, count(customerid) as countrating from movieratings where rating = 3 group by customerid, rating) r3 on r1.customerid = r3.customerid join (select customerid, count(customerid) as countrating from movieratings where rating = 4 group by customerid, rating) r4 on r1.customerid = r4.customerid join (select customerid, count(customerid) as countrating from movieratings where rating = 5 group by customerid, rating) r5 on r1.customerid = r5.customerid;

insert overwrite directory 's3://netflix-hive/output/topcustomerratingfrequency' select * from customerratingfrequency rf join (select customerid, count(rating) as countrating from movieratings group by customerid order by countrating desc limit 10) ca on rf.customerid = ca.customerid;

insert overwrite directory 's3://netflix-hive/output/ratio3' select customerid, one, five from customerratingfrequency order by one/five desc limit 10;

insert overwrite directory 's3://netflix-hive/output/ratio4' select customerid, one, five from customerratingfrequency order by one/five limit 10;

// QUERIES FOR ENRON EMAIL SET 
create external table enronset ( eid STRING,timestamp STRING,from_id STRING,to_id STRING,cc STRING,subject STRING,context STRING ) COMMENT 'enron data_set'  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'STORED AS TEXTFILE LOCATION 's3n://spring-2014-ds/enron_dataset/';

select  from_id , count(from_id) as counts  from enronset group by from_id  order by counts desc limit 10;

select from_id ,splitstring, count(1) as counts 
from enronset 
Lateral view explode(split(to_id,',')) mytable as splitstring 
where from_id="jeff.dasovich@enron.com" 
group by from_id , splitstring  
order by counts desc;

insert overwrite directory 's3n://ishan-bucket/results/'
select from_id ,splitstring, count(1) as counts 
from enronset 
Lateral view explode(split(to_id,',')) mytable as splitstring  
group by from_id , splitstring  
order by counts desc;

insert overwrite directory 's3n://ishan-bucket/results/'
select  dates , count(dates) as numEmails 
from (
select split(timestamp," ")[3] as dates , count(timestamp) 
from enronset 
group by timestamp) mailfreq 
group by dates 
order by numEmails desc;


select  dates , count(dates) from 
(select concat_ws(" " ,split(timestamp," ")[0],split(timestamp," ")[1],split(timestamp," ")[2],split(timestamp," ")[3]) as dates , count(timestamp) 
from enronset 
group by timestamp) mailfreq 
group by dates;

create table if not exists vinceemails ( from_id string , to_id string , timestamp string ,context string , hashvalue int ) row format delimited fields terminated by "\t" stored as textfile;

insert overwrite table vinceemails 
select  from_id,to_id, concat_ws(" " ,split(timestamp," ")[0],split(timestamp," ")[1],split(timestamp," ")[2],split(timestamp," ")[3]), regexp_replace(context,"\\s+",","), hash(from_id,to_id,length(context),split(timestamp," ")[4]) as identifier 
from  enronset
where split(timestamp," ")[3] = 2001 and 
from_id="vince.kaminski@enron.com";

create table vinceemailswordssplits if not exists ( form_id string , to_id string , timestamp string , context array<string> , hashvalue int ) row format delimited fields terminate by “\t”;

insert overwrite table vinceemailswordssplits select from_id, to_id,timestamp, split(context,",") , hashvalue from vinceemails;

create table afinn ( words string , sentiment int ) row format delimited fields terminated by "\t" stored as textfile;

create table if not exists vincewordsexploded ( from_id string , to_id string , timestamp string, word string , hashvalue int , sentiment int ) row format delimited fields terminated by "\t";

insert overwrite table vincewordsexploded 
select from_id ,to_id,timestamp,viral ,hashvalue, 0 
from vinceemailswordssplits 
lateral view explode(context)  mytable as viral ;

create table if not exists sentiment ( word string, timestamp string, sentiment float , hashvalue int) 
row format delimited      
fields terminated by “\t” ;

insert overwrite table sentiment 
select word,timestamp,vincewordsexploded.sentiment + afinn.sentiment as total , hashvalue 
from vincewordsexploded 
join afinn on ( vincewordsexploded.word = afinn.words) ;


insert overwrite local directory '/home/hduser/Documents/temp/sentiments'  select split(timestamp," ")[2] as mon, avg(sentiment) as avg  from sentiment group by split(timestamp," ")[2] ;
