create table users(
username varchar(50) primary key,
userurl varchar(70)
);
create table tweet (
tweetid int(40) primary key,
tweet_text text,
created_at datetime,
lang varchar(10),
conv_ID int(40),
username varchar(50),
foreign key (username) references users(username)
);
create table user_mentions(
Username varchar(70),
ID int,
quoted_tweet text,
display_name varchar(50)


/*--------------------------------------------
-------   LOADING USER.csv TABLE  -----------
---------------------------------------------*/

LOAD DATA INFILE 'C:/1st/project/user.csv' 
INTO TABLE users 
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;       
select * from labproj.users where username="mochico0123";

/*--------------------------------------------
-------  LOADING tweet.csv TABLE  -----------
---------------------------------------------*/
LOAD DATA INFILE 'C:/1st/project/tweet.csv' 
INTO TABLE tweet 
FIELDS TERMINATED BY ','
lines terminated by '\n'
IGNORE 1 ROWS; 

