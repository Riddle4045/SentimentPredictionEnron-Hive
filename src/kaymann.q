create table if not exists kayemails ( from_id string , to_id string , timestamp string ,context string , hashvalue int ) row format delimited fields terminated by "\t" stored as textfile;

insert overwrite table kayemails 
select  from_id,to_id, concat_ws(" " ,split(timestamp," ")[0],split(timestamp," ")[1],split(timestamp," ")[2],split(timestamp," ")[3]), regexp_replace(context,"\\s+",","), hash(from_id,to_id,length(context),split(timestamp," ")[4]) as identifier 
from  enronset
where split(timestamp," ")[3] = 2001 and 
from_id="kay.mann@enron.com";

create table  if not exists kayemailswordssplits  ( form_id string , to_id string , timestamp string , context array<string> , hashvalue int ) row format delimited fields terminated by “\t”;


insert overwrite table kayemailswordssplits select from_id, to_id,timestamp, split(context,",") , hashvalue from kayemails;

create table if not exists kaywordsexploded ( from_id string , to_id string , timestamp string, word string , hashvalue int , sentiment int ) row format delimited fields terminated by "\t";

insert overwrite table kaywordsexploded 
select form_id ,to_id,timestamp,viral ,hashvalue, 0 
from kayemailswordssplits 
lateral view explode(context)  mytable as viral ;

create table if not exists sentiment ( word string, timestamp string, sentiment float , hashvalue int) 
row format delimited      
fields terminated by “\t”;

insert overwrite table sentiment 
select word,timestamp,kaywordsexploded.sentiment + afinn.sentiment as total , hashvalue 
from kaywordsexploded 
join afinn on ( kaywordsexploded.word = afinn.words) ;



insert overwrite local directory '/home/hduser/Documents/temp/kaysentiments'  select split(timestamp," ")[2] as mon, avg(sentiment) as avg  from sentiment group by split(timestamp," ")[2] ;

