/*--------------------------------------------
-------   CREATING DATABASE        -----------
---------------------------------------------*/
create database twitter;
use twitter;

/*--------------------------------------
-------  CREATING  TABLES  -----------
---------------------------------------------*/

/*       Users Table      */


create table users
(
    username varchar(50) not null primary key,
    User_url varchar (50),
    user_info varchar(1000)
    );

/*  Media table    */

create table media(
media_id bigint not null primary key,
media_details text not null,
media_type varchar(10) 
);
/*       Tweet Table      */
create table tweets
(
    Tweetid bigint not null primary key,
    username varchar(20) not null,
    tweettext text not null,
    createdtime datetime not null,
    lang varchar(10) not null,
    conversation_id bigint not null,
    media_id bigint  default null,
    foreign key (username) references users(username) on delete cascade,
    foreign key (media_id) references media(media_id) 
    );

/*       Public Metrics Table      */
create table public_metrics
(
    tweetid bigint not null primary key,
    reply_count int not null default 0,
    retweet_count int not null default 0,
    like_count int not null default 0,
    quote_count int not null default 0,
    foreign key (tweetid) references tweets(tweetid) on delete cascade
);

/*       Hash tag Table      */
create table hash_tags
(
    tweetid bigint not null,
    hash_tag varchar(1000) not null,
    hastag_count int,
    foreign key (tweetid) references tweets(tweetid) on delete cascade
);


/*       Url Table      */

create table tweet_links
(
    tweetid bigint not null,
    permalink_url varchar(100) not null,
    source_url varchar(200) not null,
    count_link text not null,
    out_link text not null,
    foreign key (tweetid) references tweets(tweetid) on delete cascade
);

/* Quoted Tweet table   */
create table quoted_tweets(
tweetid bigint not null,
quoted_tweet varchar(100) not null,
foreign key (tweetid) references tweets(tweetid) on delete cascade
);

/*       User Mentions Table      */
-- User(username='fobizz', id=884708145792253952, displayname='fobizz', description=None, rawDescription=None, descriptionUrls=None, verified=None, created=None, followersCount=None, friendsCount=None, statusesCount=None, favouritesCount=None, listedCount=None, mediaCount=None, location=None, protected=None, linkUrl=None, linkTcourl=None, profileImageUrl=None, profileBannerUrl=None, label=None)
create table user_mentions
(
	tweetid bigint not null,
    username text not null, 
    foreign key (tweetid) references tweets(tweetid) on delete cascade
);


/*--------------------------------------
-------  TABLES  -----------
---------------------------------------------*/


/*     Active Users tweeted in given time in descending order     */

SELECT  username as active_users,COUNT(tweetid) as tweet_count
FROM tweets
GROUP BY username
ORDER BY COUNT(tweetid) DESC limit 10;



/*        Most Liked Tweet about ChatGPT       */

select tweetid,username,tweettext,createdtime,like_count from tweets join public_metrics using (tweetid) 
order by like_count desc; 

/*   Most Used Media Type   */

select media_type, count(*)as media_used from media 
group by media_type 
order by media_used desc;

/*   User who have used more Hashtags      */

select username,tweetid,hash_tag,hastag_count from tweets join hash_tags using (tweetid)  order by hastag_count desc;  



/*  Most Discussed tweet  with respect to replies based on conversation id  in the given data  */

SELECT tweetid,username,conversation_id,COUNT(*) as reply_to_tweet
FROM tweets
GROUP BY conversation_id
ORDER BY reply_to_tweet DESC limit 15;



/*    Most used Language to express about ChatGPT   */
select lang, count(*)as total_count from tweets group by lang order by total_count desc limit 10;













/*--------------------------------------
-------  Stored Procedures  -----------
---------------------------------------------*/


DROP PROCEDURE get_most_viral_tweet;
DELIMITER //
CREATE PROCEDURE get_most_viral_tweet()
BEGIN
    SELECT t.username,t.tweetid,t.tweettext, t.createdtime, 
    COALESCE(pm.retweet_count, 0) + COALESCE(pm.quote_count, 0)+COALESCE(pm.like_count, 0) AS total_count
    FROM tweets t
    LEFT JOIN public_metrics pm ON t.tweetid = pm.tweetid
    ORDER BY total_count DESC
    LIMIT 15;
END; //
CALL get_most_viral_tweet();


/*-----------Most used Hashtags------------*/

DROP PROCEDURE IF EXISTS GET_HASHTAG_COUNT;

DELIMITER //
	CREATE PROCEDURE GET_HASHTAG_COUNT()
	BEGIN
		
        DECLARE val TEXT DEFAULT NULL;
        DECLARE firstVal TEXT DEFAULT NULL;
        DECLARE firstValLen INT DEFAULT NULL;
        DECLARE tempVal TEXT DEFAULT NULL;
        DECLARE finished INT DEFAULT 0;
        DECLARE CUR CURSOR FOR SELECT HASH_TAG FROM hash_tags;
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET finished = 1;
        DROP TABLE IF EXISTS HASHTAG_SPLIT_TABLE;
		CREATE TABLE HASHTAG_SPLIT_TABLE(HASHTAG TEXT);
        OPEN CUR;
			get_hashtag: LOOP
			FETCH CUR INTO val;
				IF finished = 1 THEN 
				LEAVE get_hashtag;
                END IF;
			iterator: LOOP
            IF LENGTH(val) = 0 OR val IN ('[]') OR val IS NULL THEN
            LEAVE iterator;
			END IF;
			IF val NOT LIKE '[%]' THEN 
            SET val =  trim(trailing '\"' from trim(leading '\"' from val));
            END IF;
            IF val LIKE '[%]' THEN 
            SET val =  trim(trailing ']' from trim(leading '[' from val));
            END IF;
			SET firstVal = SUBSTRING_INDEX(VAL,',',1);
            SET firstValLen = LENGTH(firstVal);
			SET tempVal = trim(trailing '\'' from trim(leading '\'' from TRIM(firstVal)));
            SET tempVal = trim(trailing '\'' from trim(leading '\'' from TRIM(tempVal)));
            IF  LENGTH(tempVal) = 0 OR tempVal IS NULL THEN
            LEAVE iterator;
            END IF;
			INSERT INTO HASHTAG_SPLIT_TABLE (HASHTAG) VALUES (tempVal);
			SET val = INSERT(val,1,firstValLen + 1,'');
			END LOOP;
			END LOOP;
		CLOSE CUR;
        SELECT HASHTAG, COUNT(HASHTAG)as used_count FROM HASHTAG_SPLIT_TABLE GROUP BY HASHTAG ORDER BY COUNT(HASHTAG) DESC LIMIT 10;
    END //
DELIMITER ;

CALL GET_HASHTAG_COUNT();



/*-------------  Most used Source   -----*/

drop procedure if exists most_used_source;
DELIMITER //
CREATE PROCEDURE most_used_source()
BEGIN
SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(source_url, '>', -2), '<', 1) AS used_source, 
    COUNT(*) AS most_used_source 
FROM tweet_links
WHERE 
    source_url LIKE '%twitter.com%' 
GROUP BY used_source
ORDER BY most_used_source DESC 
LIMIT 10;
end //

call most_used_source()


/*  Most Mentioned Use     */


drop procedure if exists most_mentioned_user ;
DELIMITER //
CREATE PROCEDURE most_mentioned_user()
BEGIN
SELECT mentioned_user, COUNT(*) AS mention_count 
FROM (SELECT regexp_SUBSTR(tweettext, '@[[:alnum:]_]+') AS mentioned_user 
      FROM tweets) AS t 
WHERE mentioned_user != '' 
GROUP BY mentioned_user 
ORDER BY mention_count DESC limit 20;
end //
call most_mentioned_user()



/*--------------------------------------
-------     Triggers  ----------------------
---------------------------------------------*/

/* hashtag_count update to hash_tag table   */

drop trigger update_hastag_count;
DELIMITER //
CREATE TRIGGER update_hastag_count
AFTER INSERT ON tweets
FOR EACH ROW
BEGIN
    INSERT INTO hash_tags(tweetid,hash_tag,hastag_count)
    VALUES (NEW.tweetid,(select DISTINCT REGEXP_REPLACE(REGEXP_SUBSTR(text_column, '#\w+'), '#', '') AS hashtag),LENGTH(NEW.tweettext) - LENGTH(REPLACE(NEW.tweettext, '#', '')));
END; //
--REGEXP_SUBSTR(new.tweettext, '#[[:alnum:]_]+') 
insert into users values ('priyanka','priyanka/gowda',null);
insert into tweets values (161715357265000000,'priyanka','#chatgpt,#AI,@mochico0123 test dataon chatgpt','2023-01-26 13:44:34','en',161715357265000000,null);
insert into tweets values (161000057205020000,'priyankagowda','#chatgpt#AI,@mochico0123 test dataon chatgpt','2023-01-26 13:44:34','en',161715357265000000);
delete from tweets where username='priyankagowda';




/*-------Trigger to not update if tweet text length is > 280.......*/

drop trigger if exists trigger_tweet_length;
DELIMITER //
CREATE TRIGGER trigger_tweet_length
BEFORE insert ON tweets
FOR EACH ROW
BEGIN
    IF LENGTH(NEW.tweettext) > 280 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Tweet text cannot be longer than 280 characters';
    END IF;
END;//
insert into tweets (tweetid,tweettext) values (161050057205020000,'aaaaaaaaaaaaaaaaaaaaaaaaa
aaaaaajjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaajjjjjjjj
jjjjjjjjjjjjjjjjjjjjjjjjjjaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaajjjjjjjjjjjjjjjj
jjjjjjjjjjjjjjjjjjaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaajjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjaaaaaaaaaa
aaaaaaaaaaaaaaaaaaaaajjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj');



/*    logging into table before delete      */

CREATE TABLE log (
    delete_timestamp datetime primary key,
    db_user varchar(50),
    tweet_id bigint not null,
    username varchar(50) not null
);
DELIMITER //
CREATE TRIGGER log_before_delete
Before delete ON tweets
FOR EACH ROW
BEGIN
INSERT INTO log values (now(),user(),old.tweetid,old.username);
END;//

delete from tweets where username='priyanka';



















