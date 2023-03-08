from sqlalchemy import create_engine
import pymysql
import pandas as pd

# insert your link to the .csv file
tweet_data = pd.read_csv('C:/Users/amith/OneDrive/Desktop/Sem1/DB/LabProject/archive/chatgpt1.csv')
print(tweet_data.head())
# //username:yourPassword@localhost:portnumber/dbname
sqlEngine = create_engine('mysql+pymysql://root:NewPassword@127.0.0.1:3306/first_db', pool_recycle=3600)

dbConnection = sqlEngine.connect()
# name of the table you want to load
tableName = 'tweet_data_table'
try:

    frame = tweet_data.to_sql(tableName, con=sqlEngine, schema='first_db', if_exists='append');

except ValueError as valerror:

    print(valerror)

except Exception as e:

    print(e)

else:

    print("Table - %s created!!!." % tableName);

finally:

    dbConnection.close()
