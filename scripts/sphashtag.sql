
-- --------- 1. Stored procedure to get the Users most discussing about it on Twitter
DELIMITER //
CREATE PROCEDURE GET_USER_MOST_DISCUSSING_CHATGPT()
BEGIN
	SELECT 
		USERNAME, T.TWEET_ID
	FROM
		TWEETS T
			JOIN
		HASH_TAGS H ON T.TWEET_ID = H.TWEET_ID
	WHERE
		H.HASH_TAG LIKE '%#ChatGPT%'
	GROUP BY USERNAME
	ORDER BY T.TWEET_ID DESC
	LIMIT 10; 
END//
DELIMITER ;

CALL GET_USER_MOST_DISCUSSING_CHATGPT();

-- ------ 2. Stored procedure to get Most viral tweet (wrt retweets) about the ChatGPT.
DELIMITER //
CREATE PROCEDURE GET_MOST_VIRAL_TWEET_CHATGPT()
BEGIN
	SELECT 
    T.TWEET_ID, T.TWEET_TEXT, RETWEET_COUNT
FROM
    TWEETS T
        JOIN
    PUBLIC_METRICS P ON T.TWEET_ID = P.TWEET_ID
WHERE
    REPLYCOUNT = (SELECT 
            MAX(RETWEET_COUNT)
        FROM
            PUBLIC_METRICS);
END//
DELIMITER;

CALL GET_MOST_VIRAL_TWEET_CHATGPT();

-- ------ 3. Stored procedure to get Hashtags used prominently for the tweets about ChatGPT.
DROP TABLE IF EXISTS HASHTAG_SPLIT_TABLE;

CREATE TABLE HASHTAG_SPLIT_TABLE(HASHTAG TEXT);

DELIMITER //
	CREATE PROCEDURE GET_HASHTAG_COUNT()
	BEGIN
		
        DECLARE val TEXT DEFAULT NULL;
        DECLARE firstVal TEXT DEFAULT NULL;
        DECLARE firstValLen INT DEFAULT NULL;
        DECLARE tempVal TEXT DEFAULT NULL;
        DECLARE finished INT DEFAULT 0;
        DECLARE CUR CURSOR FOR SELECT HASTAG FROM TWEET_DATA_TABLE;
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET finished = 1;
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
			INSERT INTO HASHTAG_SPLIT_TABLE (HASHTAG) VALUES (tempVal);
			SET val = INSERT(val,1,firstValLen + 1,'');
			END LOOP;
			END LOOP;
		CLOSE CUR;
        SELECT HASHTAG, COUNT(HASHTAG) FROM HASHTAG_SPLIT_TABLE GROUP BY HASHTAG ORDER BY COUNT(HASHTAG) DESC LIMIT 10;
    END //
DELIMITER ;

CALL GET_HASHTAG_COUNT();

-- --------- 4. stored procedure to get the Most discussed tweet (wrt replies and conversation)

DELIMITER //
CREATE PROCEDURE GET_MOST_DISCUSSED_TWEET_CHATGPT()
BEGIN
	SELECT 
    T.TWEET_ID, T.TWEET_TEXT, RETWEET_COUNT
FROM
    TWEETS T
        JOIN
    PUBLIC_METRICS P ON T.TWEET_ID = P.TWEET_ID
WHERE
    REPLYCOUNT = (SELECT 
            MAX(REPLYCOUNT)
        FROM
            PUBLIC_METRICS);
END //
DELIMITER ;

CALL GET_MOST_DISCUSSED_TWEET_CHATGPT();

-- --------- 5. Stored procedure for Mentioned user table info extraction
DROP TABLE IF EXISTS MENTIONED_USERS_TABLE;

CREATE TABLE MENTIONED_USERS_TABLE(
TWEET_ID BIGINT NOT NULL PRIMARY KEY,
USERNAME VARCHAR(200),
USER_ID BIGINT
#FOREIGN KEY (TWEET_ID) REFERENCES TWEETS(TWEET_ID)
);

DELIMITER //
CREATE PROCEDURE POPULATE_USER_MENTIONED_TABLE()
BEGIN
	INSERT INTO MENTIONED_USERS_TABLE
	SELECT 
    `TWEET ID`,
    SUBSTRING(MENTIONEDUSERS, LOCATE("username='", MENTIONEDUSERS)+10, LOCATE("', id=", MENTIONEDUSERS)-LOCATE('username=', MENTIONEDUSERS)-10) AS USERNAME,
    SUBSTRING(MENTIONEDUSERS, LOCATE("', id=", MENTIONEDUSERS)+7, LOCATE(", displayname", MENTIONEDUSERS)-LOCATE("', id=", MENTIONEDUSERS)-7) AS USER_ID
	FROM TWEET_DATA_TABLE 
    WHERE MENTIONEDUSERS IS NOT NULL;
END //
DELIMITER ;

CALL POPULATE_USER_MENTIONED_TABLE();

--  ------- 6. stored procedure to get Most mentioned user
DROP TABLE IF EXISTS TOP_TEN_MOST_MENTIONED_USERS;

CREATE TABLE TOP_TEN_MOST_MENTIONED_USERS(
USERNAME VARCHAR(200) NOT NULL UNIQUE,
MENTIONED_FREQUENCY INT
);

DELIMITER //
CREATE PROCEDURE GET_TOP_TEN_MOST_MENTIONED_USERS()
BEGIN
	SELECT 
    USERNAME, COUNT(TWEET_ID)
FROM
    MENTIONED_USERS_TABLE
GROUP BY USERNAME
ORDER BY COUNT(TWEET_ID) DESC
LIMIT 10;
END //
DELIMITER ; 

CALL GET_TOP_TEN_MOST_MENTIONED_USERS();

-- -------- 7. Stored procedure to populate a source table and populate most used twitter source
DROP TABLE IF EXISTS TWEET_SOURCE_TABLE;

CREATE TABLE TWEET_SOURCE_TABLE(
TWEET_ID BIGINT,
TWEET_SOURCE VARCHAR(250));

DROP TABLE IF EXISTS SOURCE_METRICS_TABLE;

CREATE TABLE SOURCE_METRICS_TABLE(
TWEET_SOURCE VARCHAR(250),
SOURCE_COUNT INT
);

DROP FUNCTION IF EXISTS GET_SOURCE; 

-- -------- Function to extract user name and user ID from source column
DELIMITER //
CREATE FUNCTION GET_SOURCE(
			RAW_SOURCE TEXT
)
RETURNS VARCHAR(250)
DETERMINISTIC
BEGIN
	DECLARE CLEANED_SOURCE VARCHAR(250);
    SET CLEANED_SOURCE = SUBSTRING(RAW_SOURCE, LOCATE("follow\">", RAW_SOURCE)+8, LOCATE("</a>", RAW_SOURCE)-LOCATE("follow\">", RAW_SOURCE)-8);
    RETURN CLEANED_SOURCE;
END //
DELIMITER ;

DROP PROCEDURE IF EXISTS POPULATE_CLEANED_SOURCE_TABLE;

DELIMITER //
CREATE PROCEDURE POPULATE_CLEANED_SOURCE_TABLE()
BEGIN
	INSERT INTO SOURCE_METRICS_TABLE
	SELECT CLEANED_SOURCE, COUNT(CLEANED_SOURCE)
    FROM
    (SELECT 
		GET_SOURCE(`SOURCE`) AS CLEANED_SOURCE
    FROM TWEET_DATA_TABLE) CS
    GROUP BY CLEANED_SOURCE
    ORDER BY COUNT(CLEANED_SOURCE) DESC;
END //
DELIMITER ;
  
CALL POPULATE_CLEANED_SOURCE_TABLE();

-- -------- 8. Trigger to log a record in audit table when a record is deleted from tweets table

DROP TABLE IF EXISTS deleted_tweet_log;

create table deleted_tweet_log
(
	db_user varchar(20),
    delete_timestamp datetime,
    tweet_id bigint not null,
    username varchar(20) not null,
    tweet_text text not null,
    created_time datetime not null,
    `language` varchar(10) not null,
    conversation_id bigint not null,
    permalink_url varchar(100) not null,
    source_url varchar(200) not null,
    media_id bigint default null,
    primary key (db_user, delete_timestamp)
);

DROP TRIGGER IF EXISTS LOG_OF_DELETED_TWEET_RECORDS;

DELIMITER //
CREATE TRIGGER LOG_OF_DELETED_TWEET_RECORDS
    BEFORE DELETE
    ON TWEET_DATA_TABLE FOR EACH ROW
BEGIN
    INSERT INTO deleted_tweet_log (db_user, delete_timestamp, tweet_id, username, tweet_text,
    created_time, `language`, conversation_id, permalink_url, source_url, media_id) 
    VALUES (USER(), NOW(), OLD.tweet_id, OLD.username, OLD.tweet_text, OLD.created_time, 
		OLD.`Language`, OLD.conversation_id, OLD.permalink_url, OLD.source_url, OLD.media_id);
END// 
DELIMITER ;

-- ---------------- 9. create a log of user activity details
DROP TABLE IF EXISTS USER_ACTIVITY_LOG;

CREATE TABLE USER_ACTIVITY_LOG(
DB_USER VARCHAR(50),
UPDATED_TIME DATETIME);

-- --------- ENABLE GERNERAL LOGGING
SET GLOBAL general_log=1;

DROP EVENT IF EXISTS LOG_USER_ACTIVITY_EVENT;

CREATE EVENT LOG_USER_ACTIVITY_EVENT
ON SCHEDULE EVERY 1 MINUTE
STARTS CURRENT_TIMESTAMP
ENDS CURRENT_TIMESTAMP + INTERVAL 1 WEEK
DO
   INSERT INTO USER_ACTIVITY_LOG(DB_USER, UPDATED_TIME)
     SELECT USER_HOST, EVENT_TIME FROM MYSQL.GENERAL_LOG
     WHERE EVENT_TIME > CURRENT_TIMESTAMP() - INTERVAL 1 MINUTE ;

-- ---------- to disable logs
SET GLOBAL general_log=0;


-- ---------- 10. Raise error message in case of suspicious activity

DROP TRIGGER IF EXISTS SUSPICIOUS_USER_ACTIVITY_TRIGGER;

DELIMITER //
CREATE TRIGGER SUSPICIOUS_USER_ACTIVITY_TRIGGER
AFTER INSERT
ON USER_ACTIVITY_LOG FOR EACH ROW
BEGIN
	IF (TIME(NEW.UPDATED_TIME) > TIME('16:58:00') AND TIME(NEW.UPDATED_TIME) < TIME('17:50:00'))
    THEN 
		SIGNAL SQLSTATE '45000'
		SET MESSAGE_TEXT = 'ACTIVITY AT ODD TIME, SUSPICIOUS ACTIVITY LOGGED IN USER_ACTIVITY_LOG TABLE';
    END IF;
END//
DELIMITER ;
