from sqlalchemy import create_engine
import pymysql
import pandas as pd
import re

def csv_to_dataframe(path_to_csv):
    pd_dataframe = pd.read_csv(path_to_csv)
    return pd_dataframe

def df_to_sql_table(data, user_name, password, port, db_name, table_name):
    # //username:yourPassword@localhost:portnumber/dbname
    #mysql+pymysql://root:NewPassword@127.0.0.1:3306/first_db
    sql_Engine = create_engine('mysql+pymysql://'+user_name+':'+password+'@127.0.0.1:'+port+'/'+db_name, pool_recycle=3600)
    dbConnection = sql_Engine.connect()
    # name of the table you want to load
    #tableName = 'tweet_data_table'
    try:
        frame = data.to_sql(table_name, con=sql_Engine, schema=db_name, if_exists='append');
    except ValueError as valerror:
        print(valerror)
    except Exception as e:
        print(e)
    else:
        print("Table - %s created!!!." % table_name);
    finally:
        dbConnection.close()


def get_hashtags_count_table(hashtag_data):
    # data cleaning
    # removing \n and \t \r in the texts
    hashtag_data.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["", ""], regex=True, inplace=True)
    print('length of hashtag column = ', hashtags_pd.shape)
    print('No. of empty rows = ', hashtags_pd.isna().sum())
    print('No. of rows with empty brackets =', hashtags_pd[hashtags_pd.hashtag == '[]'].count())
    # total rows(50001) - rows with empty hastags(36414) = rows with hastags(13587), lets drop empty hashtag rows
    hashtag_data = hashtag_data[hashtag_data.hashtag != '[]']
    print('hashtag rows after removing empty bracket rows', hashtag_data.shape)
    hasgtag_list = list()
    for val in hashtag_data.hashtag:
        hasgtag_list.extend(re.sub(r'[\[\]\'\"]', '', val).split(','))

    new_hastag_list = list()
    for val in hasgtag_list:
        if (val != ('') and val != ('#') and val != ('##')):
            new_hastag_list.append(val.strip())

    hs = pd.DataFrame(new_hastag_list)
    hs.columns = ['hashtags']
    hs = hs[hs['hashtags'] != '']
    hs = hs[hs['hashtags'] != '#']
    hs = hs[hs['hashtags'] != '##']
    print('after doing some cleaning no. of rows in hashtag rows:', hs.shape)

    return hs

# insert your link to the .csv file
#tweet_data = pd.read_csv('C:/Users/amith/OneDrive/Desktop/Sem1/DB/LabProject/archive/chatgpt1.csv')
#print(tweet_data.head())
csv_path = 'C:/Users/amith/OneDrive/Desktop/Sem1/DB/LabProject/archive/chatgpt1.csv'
csv_data = csv_to_dataframe(csv_path)
username = 'root'
password = 'NewPassword'
port = '3306'
db_name = 'first_db'
table_name = 'tweet_data_table'
# df_to_sql_table(csv_data, username, password, port, db_name, table_name)
hashtags_pd = pd.DataFrame(csv_data['hashtag'])

hash_count_table = get_hashtags_count_table(hashtags_pd)
print(hash_count_table.head())
hashtag_table_name = 'hashtag_table'
df_to_sql_table(hash_count_table, username, password, port, db_name, hashtag_table_name)