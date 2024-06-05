*/
-------   CREATING DATABASE        -----------
---------------------------------------------*/




-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema mydb
-- -----------------------------------------------------
-- -----------------------------------------------------
-- Schema twitter
-- -----------------------------------------------------
DROP SCHEMA IF EXISTS `twitter` ;

-- -----------------------------------------------------
-- Schema twitter
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `twitter` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci ;
USE `twitter` ;

-- -----------------------------------------------------
-- Table `twitter`.`hashtags`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `twitter`.`hashtags` ;

CREATE TABLE IF NOT EXISTS `twitter`.`hashtags` (
  `hashtagId` BIGINT NOT NULL,
  `hashtag` VARCHAR(200) NOT NULL,
  PRIMARY KEY (`hashtagId`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `twitter`.`users`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `twitter`.`users` ;

CREATE TABLE IF NOT EXISTS `twitter`.`users` (
  `username` VARCHAR(50) NOT NULL,
  `User_url` VARCHAR(50) NULL DEFAULT NULL,
  `userInfo` VARCHAR(1000) NULL DEFAULT NULL,
  PRIMARY KEY (`username`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;

CREATE UNIQUE INDEX `username` ON `twitter`.`users` (`username` ASC) VISIBLE;


-- -----------------------------------------------------
-- Table `twitter`.`source`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `twitter`.`source` ;

CREATE TABLE IF NOT EXISTS `twitter`.`source` (
  `sourceId` BIGINT NOT NULL,
  `Source_url` VARCHAR(4000) NOT NULL,
  `sourceName` VARCHAR(100) NOT NULL,
  PRIMARY KEY (`sourceId`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `twitter`.`tweets`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `twitter`.`tweets` ;

CREATE TABLE IF NOT EXISTS `twitter`.`tweets` (
  `tweetId` BIGINT NOT NULL,
  `username` VARCHAR(20) NOT NULL,
  `tweetText` TEXT NOT NULL,
  `createdTime` DATETIME NOT NULL,
  `lang` VARCHAR(10) NOT NULL,
  `conversationId` BIGINT NOT NULL,
  `permalinkUrl` VARCHAR(100) NOT NULL,
  `sourceId` BIGINT NOT NULL,
  `hastagCounts` INT NULL DEFAULT '0',
  PRIMARY KEY (`tweetId`, `username`, `sourceId`),
  CONSTRAINT `tweets_ibfk_1`
    FOREIGN KEY (`username`)
    REFERENCES `twitter`.`users` (`username`),
  CONSTRAINT `tweets_ibfk_2`
    FOREIGN KEY (`sourceId`)
    REFERENCES `twitter`.`source` (`sourceId`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;

CREATE INDEX `username` ON `twitter`.`tweets` (`username` ASC) VISIBLE;

CREATE INDEX `sourceId` ON `twitter`.`tweets` (`sourceId` ASC) VISIBLE;


-- -----------------------------------------------------
-- Table `twitter`.`hashtags_usage`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `twitter`.`hashtags_usage` ;

CREATE TABLE IF NOT EXISTS `twitter`.`hashtags_usage` (
  `tweetId` BIGINT NOT NULL,
  `hashtagId` BIGINT NOT NULL,
  CONSTRAINT `hashtags_usage_ibfk_1`
    FOREIGN KEY (`tweetId`)
    REFERENCES `twitter`.`tweets` (`tweetId`),
  CONSTRAINT `hashtags_usage_ibfk_2`
    FOREIGN KEY (`hashtagId`)
    REFERENCES `twitter`.`hashtags` (`hashtagId`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;

CREATE INDEX `tweetId` ON `twitter`.`hashtags_usage` (`tweetId` ASC) VISIBLE;

CREATE INDEX `hashtagId` ON `twitter`.`hashtags_usage` (`hashtagId` ASC) VISIBLE;


-- -----------------------------------------------------
-- Table `twitter`.`media`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `twitter`.`media` ;

CREATE TABLE IF NOT EXISTS `twitter`.`media` (
  `mediaId` BIGINT NOT NULL,
  `mediaDetails` TEXT NOT NULL,
  `mediaType` VARCHAR(10) NOT NULL,
  PRIMARY KEY (`mediaId`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `twitter`.`media_usage`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `twitter`.`media_usage` ;

CREATE TABLE IF NOT EXISTS `twitter`.`media_usage` (
  `tweetId` BIGINT NOT NULL,
  `mediaId` BIGINT NOT NULL,
  CONSTRAINT `media_usage_ibfk_1`
    FOREIGN KEY (`tweetId`)
    REFERENCES `twitter`.`tweets` (`tweetId`),
  CONSTRAINT `media_usage_ibfk_2`
    FOREIGN KEY (`mediaId`)
    REFERENCES `twitter`.`media` (`mediaId`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;

CREATE INDEX `tweetId` ON `twitter`.`media_usage` (`tweetId` ASC) VISIBLE;

CREATE INDEX `mediaId` ON `twitter`.`media_usage` (`mediaId` ASC) VISIBLE;


-- -----------------------------------------------------
-- Table `twitter`.`public_metrics`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `twitter`.`public_metrics` ;

CREATE TABLE IF NOT EXISTS `twitter`.`public_metrics` (
  `tweetId` BIGINT NOT NULL,
  `replyCount` INT NOT NULL DEFAULT '0',
  `retweetCount` INT NOT NULL DEFAULT '0',
  `likeCount` INT NOT NULL DEFAULT '0',
  `quoteCount` INT NOT NULL DEFAULT '0',
  PRIMARY KEY (`tweetId`),
  CONSTRAINT `public_metrics_ibfk_1`
    FOREIGN KEY (`tweetId`)
    REFERENCES `twitter`.`tweets` (`tweetId`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `twitter`.`quoted_tweets`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `twitter`.`quoted_tweets` ;

CREATE TABLE IF NOT EXISTS `twitter`.`quoted_tweets` (
  `tweetId` BIGINT NOT NULL,
  `quotedTweet` VARCHAR(100) NOT NULL,
  CONSTRAINT `quotedTweets_ibfk_1`
    FOREIGN KEY (`tweetId`)
    REFERENCES `twitter`.`tweets` (`tweetId`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;

CREATE INDEX `tweetId` ON `twitter`.`quoted_tweets` (`tweetId` ASC) VISIBLE;


-- -----------------------------------------------------
-- Table `twitter`.`tweet_links`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `twitter`.`tweet_links` ;

CREATE TABLE IF NOT EXISTS `twitter`.`tweet_links` (
  `tweetId` BIGINT NOT NULL,
  `countLink` VARCHAR(500) NOT NULL,
  `outLink` TEXT NOT NULL,
  CONSTRAINT `tweet_links_ibfk_1`
    FOREIGN KEY (`tweetId`)
    REFERENCES `twitter`.`tweets` (`tweetId`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;

CREATE INDEX `tweetId` ON `twitter`.`tweet_links` (`tweetId` ASC) VISIBLE;


-- -----------------------------------------------------
-- Table `twitter`.`user_mentions`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `twitter`.`user_mentions` ;

CREATE TABLE IF NOT EXISTS `twitter`.`user_mentions` (
  `tweetId` BIGINT NOT NULL,
  `username` VARCHAR(20) NOT NULL,
  CONSTRAINT `user_mentions_ibfk_1`
    FOREIGN KEY (`tweetId`)
    REFERENCES `twitter`.`tweets` (`tweetId`),
  CONSTRAINT `user_mentions_ibfk_2`
    FOREIGN KEY (`username`)
    REFERENCES `twitter`.`users` (`username`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;

CREATE INDEX `tweetId` ON `twitter`.`user_mentions` (`tweetId` ASC) VISIBLE;

CREATE INDEX `username` ON `twitter`.`user_mentions` (`username` ASC) VISIBLE;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;

/*----------------------------------------------------------
--------------------------QUERIES---------------------------
----------------------------------------------------------*/

/*     Active Users tweeted in given time in descending order     */

SELECT  username as active_users, COUNT(tweetid) as tweet_count
FROM tweets
GROUP BY username
ORDER BY COUNT(tweetid) DESC limit 10;

/*  Most Discussed tweet  with respect to replies based on conversation id  in the given data  */

Select 
	tweets.tweetid, tweets.tweettext, tweets.username, reply_to_tweet
from
	tweets join
	(
		SELECT 
			tweetId, username, conversationId, 
			COUNT(tweetId) over (partition by conversationId) as reply_to_tweet
		FROM tweets
		ORDER BY reply_to_tweet DESC
    ) converation using(tweetId)
where tweets.tweetId = tweets.conversationId;

/*    Tweets on ChatGPT based on Used Language   */
select 
	lang, count(*)as total_count 
from 
	tweets 
group by lang 
order by total_count desc limit 10;


/*        Most Liked Tweet about ChatGPT       */

select tweetid,username,tweettext,createdtime,likecount from tweets join public_metrics using (tweetid) 
order by likecount desc; 


/*   Most Used Media Type   */

select mediatype, count(*)as media_used from media 
group by mediatype 
order by media_used desc;


/*   Users with more No of retweets    */

select username,tweetid,retweetcount from tweets join public_metrics 
using (tweetid)  order by retweetcount desc; 

/* number of Tweets on Chat GPT */
select count(*) as number_of_tweets_on_Chatgpt from tweets 
where tweettext like '%chatgpt%'; 
 
/* Most retweeted tweet with the comments */
select username,tweetid,quotecount as retweet_with_comments
 from tweets join public_metrics 
using (tweetid)  order by retweet_with_comments desc; 

-- conversations with most number of media shares
select conversationid, mediatype, count(mediaid) as `number of media shared`
from tweets join media_usage using (tweetid) join media using (mediaid)
group by conversationid, mediatype
order by count(mediaid) desc
limit 10;



/*  Most retweeted tweet in each language  */
with langquery as (
select tweetid, `lang`, username, tweettext, retweetcount, 
row_number() over (partition by `lang` order by retweetcount desc) as rownum
from tweets join public_metrics using (tweetid))
Select * from langquery
where rownum =1
order by retweetcount desc;

/*   top 10 Users who have more outsource for reference on the tweet  */
select username, count(outlink) as `Number of links` 
from tweets join tweet_links using (tweetid)
group by username
order by count(outlink) desc
limit 10;


-- Number of tweets each day on chatGPT
select date(createdtime) as Tweeted_date, count(tweetid) as tweet_count 
from tweets 
group by Tweeted_date;



/*--------------------------------------
-------  Stored Procedures  -----------
---------------------------------------------*/
DROP PROCEDURE get_most_viral_tweet;
DELIMITER //
	CREATE PROCEDURE get_most_viral_tweet()
	BEGIN
		SELECT 
			t.tweetId, t.username, t.tweetText, t.createdTime, t.lang, t.conversationId, t.permalinkUrl, t.sourceId, 
			COALESCE(pm.retweetCount, 0) + COALESCE(pm.quoteCount, 0) + COALESCE(pm.likeCount, 0) AS viralCount
		FROM 
			tweets t LEFT JOIN public_metrics pm ON t.tweetId = pm.tweetId
		ORDER BY viralCount DESC
		LIMIT 15;
		END; //
CALL get_most_viral_tweet();


/*-----------Most used Hashtags------------*/

DROP PROCEDURE IF EXISTS GET_HASHTAG_COUNT;
DELIMITER //
	CREATE PROCEDURE GET_HASHTAG_COUNT()
	BEGIN
		SELECT 	h.hashtag, count(tweetId) as total_count
		FROM hashtags h join hashtags_usage hu using (hashtagId)
		group by h.hashtagId
		order by total_count desc;
END //
DELIMITER ;
CALL GET_HASHTAG_COUNT();

/*-------------  Most used Source   -----*/
drop procedure if exists most_used_source;
DELIMITER //
	CREATE PROCEDURE most_used_source()
	BEGIN
		SELECT s.SourceName as Source_Name, count(t.tweetId) as Total_Count
		FROM tweets t JOIN `source` s using (sourceId)
		GROUP BY s.SourceName
        ORDER BY Total_Count desc
        LIMIT 10;
        end //
DELIMITER ;
call most_used_source()

/*  Most Mentioned User     */
drop procedure if exists most_mentioned_user ;
DELIMITER //
	CREATE PROCEDURE most_mentioned_user()
	BEGIN
		select 	username, count(tweetId) as Total_Mentions
		from user_mentions
		group by username
        order by Total_Mentions desc
        limit 10;
end //
DELIMITER ;;
call most_mentioned_user()




/*--------------------------------------
-------     Triggers  ----------------------
---------------------------------------------*/
/* prepeartion for deleting a tweet from tweet table */
DROP TRIGGER IF EXISTS prepare_before_deleting_tweet;
DELIMITER //
	CREATE TRIGGER prepare_before_deleting_tweet
	Before delete ON tweets
	FOR EACH ROW
	BEGIN
		-- Delete from user mentions table
        DELETE from user_mentions where tweetId = old.tweetId;
        -- Delte from hashtag usage table
        DELETE from hashtags_usage where tweetId = old.tweetId;
        -- Delete from media usage table
        DELETE from media_usage where tweetId = old.tweetId;
        -- Delete from media usage table
        DELETE from public_metrics where tweetId = old.tweetId;
        -- Delete from media usage table
        DELETE from tweet_links where tweetId = old.tweetId;
	END;//
DELIMITER ;;

delete from tweets where tweetId = '1617156404137295878';
select * from tweets where tweetId = 1617156404137295878;


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
insert into tweets (tweetid,tweettext) values (161050057205020000,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaajjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj
aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaajjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjaaaaaaaaaaaaaaaaaaaaaaaaaaaa
aaajjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaajjjjjjjjjjjjjjjjjjjjjjjjjjjj
jjjjjjaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaajjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjaa
aaaaaaaaaaaaaaaaaaaaaaaaaaaaajjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj');







/*    logging into table before delete      */
/* prepeartion for deleting a tweet from tweet table */
CREATE TABLE log (
    delete_timestamp datetime primary key,
    db_user varchar(50),
    tweet_id bigint not null,
    username varchar(50) not null
);
DROP TRIGGER IF EXISTS prepare_before_deleting_tweet;
DELIMITER //
	CREATE TRIGGER prepare_before_deleting_tweet
	Before delete ON tweets FOR EACH ROW
	BEGIN
		DELETE from user_mentions where tweetId = old.tweetId;  -- Delete from user mentions table
        DELETE from hashtags_usage where tweetId = old.tweetId;  -- Delte from hashtag usage table
        DELETE from media_usage where tweetId = old.tweetId;  -- Delete from media usage table
        DELETE from public_metrics where tweetId = old.tweetId;  -- Delete from public metric table
        DELETE from tweet_links where tweetId = old.tweetId;  -- Delete from tweet_links table
        INSERT INTO log values (now(), user(), old.tweetid,old.username);
	END;//
DELIMITER ;;
insert into users(username, User_Url) values ('group7','https://twitter.com/group7');
insert into twitter.tweets values 
(161715357265000000,'group7','test data on chatgpt','2023-01-26 13:44:34','en', 161715357265000000, 
'https://www.twitter.com/priyanka/status/161715357265000000', 2046924143748526506, 0);
select *  from tweets where tweetid=161715357265000000;
delete from tweets where username='group7';
select * from log where  username='group7';





Select * from mysql.slow_log
Select * from mysql.general_log








