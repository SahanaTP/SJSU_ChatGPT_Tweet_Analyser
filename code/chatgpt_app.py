import getpass
import sys
import argparse
import pymysql
import signal
import pandas as pd


def method_help():
    print("\n")
    print("----------------------------------------")
    print("    method_id        |  method_type")
    print("----------------------------------------")
    for key, value in method_callback.items():
        print("         {idx}           | {help} ".format(idx=key, help=value.get('help')))
    print("----------------------------------------")
    print("\n")


class Password(argparse.Action):
    def __call__(self, parser, namespace, values, option_string):
        values = getpass.getpass()
        setattr(namespace, self.dest, values)


def setup_args(parser):
    if not parser:
        return False
    parser.add_argument('-u', '--username', help="Host User Name", const=1, default='admin', nargs="?", type=str)
    parser.add_argument('-p', '--password', help="Host User Password", action=Password, nargs='?', dest='password',
                        const=1, required=True)
    parser.add_argument('-e', '--endpoint', help="AWS RDS id", const=1,
                        default='awslab.c0ecayyvk7tw.us-east-2.rds.amazonaws.com', nargs="?", type=str)
    parser.add_argument('-d', '--database', help="DB name", const=1, default='twitter', nargs="?", type=str)


def make_connection_to_rds(user, password, host):
    conn = None
    try:
        conn = pymysql.connect(host=host,
                               user=user,
                               password=password)
    except pymysql.Error as e:
        print("make_connection_to_rds: Failed to connect to the AWS RDS")
        return conn
    else:
        return conn


def close_connection_to_rds():
    try:
        conn.close()
    except pymysql.Error as e:
        print("close_connection_to_rds: Failed to close connection to the AWS RDS")
    else:
        print("close_connection_to_rds: Successfully closed connection to the AWS RDS")


def execute_sql_query(sql):
    try:
        cursor.execute(sql)
    except (pymysql.Error, pymysql.Warning) as e:
        print("execute_sql_query: Failed to execute sql query {sql}".format(sql=sql))
        return False
    return True


def show_data_bases():
    sql = '''SHOW DATABASES'''
    rc = execute_sql_query(sql)
    if not rc:
        return False

    databases = cursor.fetchall()
    print("|    Database name         |")
    print("----------------------------")
    idx = 1
    for database in databases:
        print("{idx}. {db} ".format(idx=idx, db=database[0]))
        idx += 1
    return True


def use_data_base(database):
    return execute_sql_query(sql='''USE {db}'''.format(db=database))


def get_most_viral_tweet_sp():
    sql = '''CALL get_most_viral_tweet();'''
    rc = execute_sql_query(sql)
    if not rc: return False

    var = cursor.fetchall()
    df = pd.DataFrame(var, columns=['tweetId', 'username', 'tweetText', 'createdTime', 'lang',
                                    'conversationId', 'permalinkUrl', 'sourceId', 'viralCount'])
    print(df)
    return True


def get_most_used_hashtags_sp():
    sql = '''CALL GET_HASHTAG_COUNT();'''
    rc = execute_sql_query(sql)
    if not rc: return False

    var = cursor.fetchall()
    df = pd.DataFrame(var, columns=['Hashtag', 'Hashtag_count'])
    print(df.head(10))
    return True


def get_most_used_source_sp():
    sql = '''CALL most_used_source();'''
    rc = execute_sql_query(sql)
    if not rc: return False

    var = cursor.fetchall()
    df = pd.DataFrame(var, columns=['source', 'count'])
    print(df)
    return True


def get_most_mentioned_user_sp():
    sql = '''CALL most_mentioned_user();'''
    rc = execute_sql_query(sql)
    if not rc: return False

    var = cursor.fetchall()
    df = pd.DataFrame(var, columns=['mentioned_user', 'count'])
    print(df)
    return True


def get_most_actv_usrs_in_desc_order_q():
    sql = \
        '''
    SELECT  username as active_users, COUNT(tweetid) as tweet_count
    FROM tweets
    GROUP BY username
    ORDER BY COUNT(tweetid) DESC limit 10;
    '''
    rc = execute_sql_query(sql)
    if not rc: return False

    var = cursor.fetchall()
    df = pd.DataFrame(var, columns=['active_users', 'tweet_count'])
    print(df)
    return True


def get_most_liked_tweet_abt_chatgpt_q():
    sql = \
        '''
    select tweetid,username,tweettext,createdtime,likecount 
    from tweets 
    join public_metrics using (tweetid) 
    order by likecount desc;  
    '''
    rc = execute_sql_query(sql)
    if not rc: return False

    var = cursor.fetchall()
    df = pd.DataFrame(var, columns=['tweetid', 'username', 'tweettext', 'createdtime', 'like_count'])
    print(df.head(10))
    return True


def get_most_used_media_type_q():
    sql = \
        '''
    select mediatype, count(*)as media_used from media 
    group by mediatype 
    order by media_used desc;
    '''
    rc = execute_sql_query(sql)
    if not rc: return False

    var = cursor.fetchall()
    df = pd.DataFrame(var, columns=['media_type', 'media_used'])
    print(df)
    return True


def get_users_with_more_No_of_retweets_q():
    sql = \
        '''
    select username, tweetid, retweetcount 
    from tweets join public_metrics 
    using (tweetid)  
    order by retweetcount desc 
    limit 10;  
    '''
    rc = execute_sql_query(sql)
    if not rc: return False

    var = cursor.fetchall()
    df = pd.DataFrame(var, columns=['username', 'tweetid', 'retweetcount'])
    print(df)
    return True


def get_most_talked_tweets_wrt_replies_q():
    sql = \
        '''
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
    '''
    rc = execute_sql_query(sql)
    if not rc: return False

    var = cursor.fetchall()
    df = pd.DataFrame(var, columns=['tweetid', 'tweettext', 'username', 'reply_to_tweet'])
    print(df.head(10))
    return True


def number_of_Tweets_on_ChatGPT_q():
    sql = \
        '''
    select count(*) as number_of_tweets_on_Chatgpt 
    from tweets 
    where tweettext 
    like '%chatgpt%';
    '''
    rc = execute_sql_query(sql)
    if not rc: return False

    var = cursor.fetchall()
    df = pd.DataFrame(var, columns=['number_of_tweets_on_Chatgpt'])
    print(df)
    return True


def get_most_retweeted_in_each_lang_q():
    sql = \
        '''
    with langquery as (
    select tweetid, `lang`, username, tweettext, retweetcount, 
    row_number() over (partition by `lang` order by retweetcount desc) as rownum
    from tweets join public_metrics using (tweetid))
    Select * from langquery
    where rownum =1
    order by retweetcount desc;
    '''
    rc = execute_sql_query(sql)
    if not rc: return False

    var = cursor.fetchall()
    df = pd.DataFrame(var, columns=['tweetid', 'lang', 'username', 'tweettext', 'retweet_count', 'rownum'])
    print(df.head(10))
    return True


def get_top_user_who_have_more_outlink_q():
    sql = \
        '''
    select username, count(outlink) as `Number of links` 
    from tweets join tweet_links using (tweetid)
    group by username
    order by count(outlink) desc
    limit 10;
    '''
    rc = execute_sql_query(sql)
    if not rc: return False

    var = cursor.fetchall()
    df = pd.DataFrame(var, columns=['username', 'Number_of_links'])
    print(df)
    return True


def get_conv_with_most_media_share_q():
    sql = \
        '''
    select conversationid, mediatype, count(mediaid) as `number of media shared`
    from tweets join media_usage using (tweetid) join media using (mediaid)
    group by conversationid, mediatype
    order by count(mediaid) desc
    limit 10;
    '''
    rc = execute_sql_query(sql)
    if not rc: return False

    var = cursor.fetchall()
    df = pd.DataFrame(var, columns=['conversation_id', 'media_type', 'number_of_media_shared'])
    print(df)
    return True


def get_number_of_tweets_in_a_day_q():
    sql = \
        '''
    select date(createdtime) as Tweeted_date, count(tweetid) as tweet_count 
    from tweets 
    group by Tweeted_date;
    '''
    rc = execute_sql_query(sql)
    if not rc: return False

    var = cursor.fetchall()
    df = pd.DataFrame(var, columns=['Tweeted_date', 'tweet_count'])
    print(df)
    return True


def get_most_retweeted_tweet_with_the_comments_q():
    sql = \
        '''
    select username, tweetid, tweettext, quotecount as retweet_with_comments
    from tweets join public_metrics 
    using (tweetid)  
    order by retweet_with_comments desc
    limit 10; 
    '''
    rc = execute_sql_query(sql)
    if not rc: return False

    var = cursor.fetchall()
    df = pd.DataFrame(var, columns=['username', 'tweetid', 'tweetText', 'retweet_with_comments'])
    print(df)
    return True


def _chatgpt_exit():
    pass


method_callback = \
    {
        # Stored procedures
        '1': {'func_p': get_most_viral_tweet_sp, 'help': 'Get most viral tweet'},
        '2': {'func_p': get_most_used_hashtags_sp, 'help': 'Get most used hashtags'},
        '3': {'func_p': get_most_used_source_sp, 'help': 'Get most used source'},
        '4': {'func_p': get_most_mentioned_user_sp, 'help': 'Get most mentioned user'},
        # SQL queries
        '5': {'func_p': get_most_actv_usrs_in_desc_order_q, 'help': 'Get most active users in descending order'},
        '6': {'func_p': get_most_liked_tweet_abt_chatgpt_q, 'help': 'Get most liked tweet about chatgpt'},
        '7': {'func_p': get_most_used_media_type_q, 'help': 'Get most used media type'},
        '8': {'func_p': get_users_with_more_No_of_retweets_q, 'help': 'Get Users with more No of retweets'},
        '9': {'func_p': get_most_talked_tweets_wrt_replies_q,
              'help': 'Get most Discussed tweet  with respect to replies based on conversation id '},
        '10': {'func_p': number_of_Tweets_on_ChatGPT_q, 'help': 'number of Tweets on Chat GPT'},
        '11': {'func_p': get_most_retweeted_in_each_lang_q, 'help': 'Get most retweeted tweet in each language'},
        '12': {'func_p': get_top_user_who_have_more_outlink_q,
               'help': 'Get top 10 Users who have more outsource for reference on the tweet'},
        '13': {'func_p': get_conv_with_most_media_share_q,
               'help': 'Get conversations with most number of media shares'},
        '14': {'func_p': get_number_of_tweets_in_a_day_q, 'help': 'Get number of tweets in a day on ChatGpt'},
        '15': {'func_p': get_most_retweeted_tweet_with_the_comments_q,
               'help': 'Get most retweeted tweet with the comments'},

        # Dummy exit option
        'exit': {'func_p': _chatgpt_exit, 'help': 'Exits program'}
    }


def _sjsu_chatgpt_analyzer(args):
    global cursor
    global conn
    number_of_queries = 0

    conn = make_connection_to_rds(args.username, args.password, args.endpoint)
    if not conn:
        print("Failed to make connection to the AWS RDS")
        sys.exit(0)

    cursor = conn.cursor()

    def signal_handler(sig, frame):
        if conn:
            close_connection_to_rds()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    show_data_bases()
    use_data_base(args.database)

    # Make sure that the cursor is pointing to the correct database
    if cursor._last_executed != '''USE {db}'''.format(db=args.database):
        print("_sjsu_chatgpt_analyzer: Failed to use table")

    while True:
        method_help()
        method_name = input("Enter the method_name: ")
        if method_name == 'exit':
            break
        elif method_name == "":
            pass
        elif (method_callback.get(method_name) and
              method_callback.get(method_name).get('func_p') and
              method_name != 'exit'):
            print(" ##################################### {name} ##################################### \n".
                  format(name=method_callback.get(method_name).get('help')))

            rc = method_callback[method_name]['func_p']()
            print("#" * 100)
            if not rc:
                print("Failed to execute method_name {}".format(method_name))
        else:
            print("Invalid option {}".format(method_name))
        number_of_queries += 1

    close_connection_to_rds()
    print("Thank you for using ChatGpt Analyzer! Number of queries executed {}".format(number_of_queries))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Welcome to SJSU ChatGpt Analyzer')
    setup_args(parser)
    args = parser.parse_args()

    _sjsu_chatgpt_analyzer(args)
