#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np


# In[2]:


df = pd.read_csv("chatgpt1.csv")
df.shape


# In[4]:


db_user="admin"
db_password="labproject"
db_name="twitter"


# In[13]:



from urllib.parse import quote
from sqlalchemy import create_engine
from sqlalchemy import text

engine = create_engine(f"mysql+pymysql://admin:{quote(db_password)}@awslab.c0ecayyvk7tw.us-east-2.rds.amazonaws.com:3306")


# In[ ]:





# In[14]:


columns_map = {'Datetime' : 'createdTime', 'Text': 'tweetText', 'Username' : 'username', 
               'Permalink': 'permalinkUrl', 'User' : 'userUrl', 'Outlinks': 'outLink',
               'CountLinks': 'countLink', 'ReplyCount' : 'replyCount', 'RetweetCount': 'retweetCount', 
               'LikeCount': 'likeCount', 'QuoteCount': 'quoteCount', 'ConversationId': 'conversationId', 
               'Language': 'lang', 'Source': 'sourceUrl', 'Media': 'mediaDetails', 
               'QuotedTweet': 'quotedTweet', 'MentionedUsers': 'userInfo', 'hash_tag' : 'hashtag', 
               'hastag_counts': 'hastagCounts', 'Tweet Id': 'tweetId' }
df = df.rename(columns=columns_map)
df.fillna('', inplace=True)
df.columns


# In[15]:


df.fillna(np.nan, inplace=True)


# In[16]:


df['mediaId'] = df.index
df['mediaId'] = df['mediaId'].where(df['mediaDetails']!='', np.nan) 
df['sourceId'] = abs(df.Source_url.map(hash))


# In[17]:


def convert_to_list(value):
    return value.replace('[', '').replace(']', '').replace('\'', '').replace(' ', '').split(",")


# In[19]:


def popualate_table(df, table_name):
    with engine.begin() as connection:
        df.to_sql(table_name, 
                  con = connection, 
                  schema=db_name, 
                  if_exists = 'append', 
                  chunksize = 1000, 
                  index = False)


# In[27]:


'''
create table users
(
    username varchar(20) not null primary key,
    userUrl varchar (50)
);
'''

def get_users(value):
    value = value.replace('[', '').replace(']', '').replace(' ', '').replace('\"', '').replace('\'', '')
    users = []
    for user in value.split(',User('):
        user = user.replace('User(', '').replace('(', '').replace(')', '') 
        users.append(user)
    return users

def get_user_name(info):
    return info.split(',')[0].split('username=')[1].replace('\'', '')
                                            
columns = ['username', 'User_url']
user_df = df[columns]
user_df = user_df.drop_duplicates()

xtra_user = df[['userInfo']].dropna().drop_duplicates()
xtra_user = xtra_user[xtra_user['userInfo'] != '']
xtra_user['userInfo'] = [get_users(x) for x in  xtra_user['userInfo']]
xtra_user = xtra_user.explode('userInfo')
xtra_user = xtra_user[xtra_user['userInfo'] != np.nan].drop_duplicates()
xtra_user = xtra_user.dropna().drop_duplicates()
xtra_user['username'] = [get_user_name(x) for x in xtra_user['userInfo']]
xtra_user['User_url'] = 'https://twitter.com/' + xtra_user['username']
xtra_user = xtra_user.drop_duplicates(subset=['username', 'User_url'], keep='last')

all_user_df = pd.merge(user_df, xtra_user, how="outer", on=["username", "User_url"]).drop_duplicates()
all_user_df.head()
popualate_table(all_user_df, 'users')


# In[28]:


'''
create table media
(
    mediaId bigint not null primary key,
    mediaDetails text not null,
    mediaType varchar(10) default null,
);
'''

def clear_field(x):
    return x.replace('[', '').replace(']', '').strip()

def get_all_media(mediaDetails):
    x = mediaDetails.replace('[', '').replace(']', '').replace(' ', '')
    x = x.replace('Photo(', 'Photo(Photo(')
    x = x.replace('Video(', 'Video(Video(')
    x = x.replace('Gif(', 'Gif(Gif(')
    
    res = [x]
    for s in ['),Photo(', '), Video(', '), Gif(']:
        temp = [p for r in res for p in r.split(s)]
        res = temp
    
    result = []
    for r in res:
        r = r.replace('Photo(Photo(', 'Photo(')
        r = r.replace('Video(Video(', 'Video(')
        r = r.replace('Gif(Gif(', 'Gif(')
        if not r.endswith(')'):
            r = r+')'
        result.append(r)
    return result
    

def get_mediaType(media):
    return media.replace('[', '').replace(']', '').replace(' ', '').split('(')[0]


pd.set_option('display.max_colwidth', None)

columns = ['Tweetid', 'mediaDetails']
media_df = df[columns]
media_df['mediaDetails'] = [clear_field(x) for x in media_df['mediaDetails']]
media_df['mediaDetails'] = media_df['mediaDetails'].where(media_df['mediaDetails'] != '', np.nan)
media_df = media_df.dropna().drop_duplicates()


media_df['mediaDetails'] = [get_all_media(x) for x in media_df['mediaDetails']]
media_df = media_df.explode('mediaDetails')

media_df['mediaType'] = [get_mediaType(x) for x in media_df['mediaDetails']]

media_df = media_df.dropna().drop_duplicates()
media_df['mediaId'] = abs(media_df.mediaDetails.map(hash))



media_info_df = media_df[['mediaId', 'mediaType', 'mediaDetails']].drop_duplicates()
media_info_df = media_info_df.drop_duplicates(subset=['mediaId'], keep='last')
media_info_df.head()

media_usage_df = media_df[['Tweetid', 'mediaId']].dropna().drop_duplicates()
media_usage_df.head()
popualate_table(media_info_df, 'media')


# In[29]:


'''
    "<a href=""http://twitter.com/download/iphone"" rel=""nofollow"">Twitter for iPhone</a>"
'''
def get_source_name(x):
    return x.split(">")[1].split("<")[0].strip()

source_df = df[['sourceId', 'Source_url']]
source_df['sourceName'] = [get_source_name(x) for x in df['Source_url']]
source_df = source_df.drop_duplicates(subset=['sourceId'], keep='last')
source_df.shape
popualate_table(source_df, 'source')


# In[ ]:





# In[30]:


'''
create table tweets
(
    tweetId bigint not null primary key,
    username varchar(20) not null,
    tweetText text not null,
    createdTime datetime not null,
    lang varchar(10) not null,
    conversationId bigint not null, -- this is similar to tweetId and need to attached
    permalinkUrl varchar(100) not null,
    sourceId int not null,
    mediaId bigint default null,
    hastagCount int not null default 0,
    foreign key (username) references users(username),
    foreign key (mediaId) references media(mediaId)
);
'''
columns = ['Tweetid', 'username', 'Tweettext', 'createdtime', 'Lang', 'conversationId', 'permalinkUrl',
          'sourceId', 'hastagCounts']
tweet_df = df[columns].drop_duplicates()
tweet_df.head()
popualate_table(tweet_df, 'tweets')


# In[31]:


'''
create table public_metrics
(
    tweetId bigint not null primary key,
    replyCount int not null default 0,
    retweetCount int not null default 0,
    likeCount int not null default 0,
    quoteCount int not null default 0,
    foreign key (tweetId) references tweets(tweetId)
);
'''
columns = ['Tweetid', 'replyCount', 'retweetCount', 'likeCount', 'quoteCount']
metric_df = df[columns].drop_duplicates()
metric_df.dtypes
for c in columns:
    metric_df[c] = metric_df[c].astype(object)
popualate_table(metric_df, 'public_metrics')


# In[33]:


'''
create table hash_tags
(
    tweetId bigint not null,
    hash_tag varchar(100) not null,
    foreign key (tweetId) references tweets(tweetId)
);
'''
columns = ['Tweetid', 'hashtag']
hashtag_df = df[columns]

hashtag_df['hashtag'] = [convert_to_list(x) for x in  hashtag_df['hashtag']]
hashtag_df = hashtag_df.explode('hashtag')
hashtag_df = hashtag_df[hashtag_df['hashtag'] != ''].drop_duplicates()

hashtag_df['hashtagId'] = abs(hashtag_df.hashtag.map(hash))
hashtag_info_df = hashtag_df[['hashtagId', 'hashtag']].dropna().drop_duplicates()
hashtag_usage_df = hashtag_df[['Tweetid', 'hashtagId']].dropna().drop_duplicates()

hashtag_usage_df['Tweetid'] = hashtag_usage_df['Tweetid'].astype(object)
hashtag_usage_df['hashtagId'] = hashtag_usage_df['hashtagId'].astype(object)

hashtag_usage_df.head()
popualate_table(hashtag_info_df, 'hashtags')


# In[40]:


popualate_table(hashtag_usage_df, 'hashtags_usage')


# In[35]:


'''
create table tweet_links
(
    tweetId bigint not null,
    countLink varchar(100) not null,
    outLink varchar(500) not null,
    foreign key (tweetId) references tweets(tweetId)
);
'''
columns = ['Tweetid', 'countLink', 'outLink']
tweet_links_df = df[columns]
tweet_links_df['countLink'] = [convert_to_list(x) for x in  tweet_links_df['countLink']]
tweet_links_df['outLink'] = [convert_to_list(x) for x in  tweet_links_df['outLink']]

tweet_links_df = tweet_links_df.explode('countLink').explode('outLink')
tweet_links_df = tweet_links_df[(tweet_links_df['countLink'] != '') & 
                                (tweet_links_df['outLink'] != '')]
tweet_links_df.head()
popualate_table(tweet_links_df, 'tweet_links')


# In[37]:


'''

create table quotedTweets(
    tweetId bigint not null,
    quotedTweet varchar(100) not null,
    foreign key(tweetId) references tweets(tweetId)
);
'''
columns = ['Tweetid', 'quotedTweet']
quoted_tweets_df = df[columns].drop_duplicates().dropna()
quoted_tweets_df = quoted_tweets_df[quoted_tweets_df['quotedTweet'] != '']
quoted_tweets_df.head()
popualate_table(quoted_tweets_df, 'quoted_tweets')


# In[38]:


'''
-- User(username='fobizz', id=884708145792253952, displayname='fobizz', description=None, rawDescription=None, 
descriptionUrls=None, verified=None, created=None, followersCount=None, friendsCount=None, statusesCount=None, 
favouritesCount=None, listedCount=None, mediaCount=None, location=None, protected=None, linkUrl=None, 
linkTcourl=None, profileImageUrl=None, profileBannerUrl=None, label=None)

create table user_mentions
(
    tweetId bigint not null,
    userInfo text not null, 
    foreign key (tweetId) references tweets(tweetId)
);
'''
'''
'''

def get_users(value):
    value = value.replace('[', '').replace(']', '').replace(' ', '').replace('\"', '').replace('\'', '')
    users = []
    for user in value.split(',User('):
        user = user.replace('User', '').replace('(', '').replace(')', '') 
        users.append(user)
    return users

def get_user_name(info):
    return info.split(',')[0].split('username=')[1].replace('\'', '')
                                            

columns = ['Tweetid', 'userInfo']
user_mentions_df = df[columns].drop_duplicates()

user_mentions_df['userInfo'] = [get_users(x) for x in  user_mentions_df['userInfo']]
user_mentions_df = user_mentions_df.explode('userInfo')
user_mentions_df = user_mentions_df[user_mentions_df['userInfo'] != ''].drop_duplicates()
user_mentions_df['username'] = [get_user_name(x) for x in user_mentions_df['userInfo']]


mentions_df = user_mentions_df[['Tweetid', 'username']]
mentions_df.head()
popualate_table(mentions_df, 'user_mentions')


# In[39]:


'''
create table media
(
    mediaId bigint not null primary key,
    mediaDetails text not null,
    mediaType varchar(10) default null,
);
'''

def clear_field(x):
    return x.replace('[', '').replace(']', '').strip()

def get_all_media(mediaDetails):
    x = mediaDetails.replace('[', '').replace(']', '').replace(' ', '')
    x = x.replace('Photo(', 'Photo(Photo(')
    x = x.replace('Video(', 'Video(Video(')
    x = x.replace('Gif(', 'Gif(Gif(')
    
    res = [x]
    for s in ['),Photo(', '), Video(', '), Gif(']:
        temp = [p for r in res for p in r.split(s)]
        res = temp
    
    result = []
    for r in res:
        r = r.replace('Photo(Photo(', 'Photo(')
        r = r.replace('Video(Video(', 'Video(')
        r = r.replace('Gif(Gif(', 'Gif(')
        if not r.endswith(')'):
            r = r+')'
        result.append(r)
    return result
    

def get_mediaType(media):
    return media.replace('[', '').replace(']', '').replace(' ', '').split('(')[0]


pd.set_option('display.max_colwidth', None)

columns = ['Tweetid', 'mediaDetails']
media_df = df[columns]
media_df['mediaDetails'] = [clear_field(x) for x in media_df['mediaDetails']]
media_df['mediaDetails'] = media_df['mediaDetails'].where(media_df['mediaDetails'] != '', np.nan)
media_df = media_df.dropna().drop_duplicates()


media_df['mediaDetails'] = [get_all_media(x) for x in media_df['mediaDetails']]
media_df = media_df.explode('mediaDetails')

media_df['mediaType'] = [get_mediaType(x) for x in media_df['mediaDetails']]

media_df = media_df.dropna().drop_duplicates()
media_df['mediaId'] = abs(media_df.mediaDetails.map(hash))



mdeia_info_df = media_df[['mediaId', 'mediaType', 'mediaDetails']]
mdeia_info_df.head()
media_usage_df = media_df[['Tweetid', 'mediaId']].dropna().drop_duplicates()

media_usage_df['Tweetid'] = media_df['Tweetid'].astype(object)
media_usage_df['mediaId'] = media_df['mediaId'].astype(object)

media_usage_df.head()
popualate_table(media_usage_df, 'media_usage')


# In[32]:


'''
    "<a href=""http://twitter.com/download/iphone"" rel=""nofollow"">Twitter for iPhone</a>"
'''
def get_source_name(x):
    return x.split(">")[1].split("<")[0].strip()

source_df = df[['sourceId', 'Source_url']]
source_df['sourceName'] = [get_source_name(x) for x in df['Source_url']]
source_df = source_df.drop_duplicates(subset=['sourceId'], keep='last')
source_df.shape


# In[ ]:





# In[ ]:




