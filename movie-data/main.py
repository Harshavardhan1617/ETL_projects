import sqlalchemy
import pandas as pd
#import json
import datetime
from sqlalchemy.orm import sessionmaker
import sqlite3
#import requests
import twint

DATABASE_LOCATION = "sqlite:///movies.sqlite"


def check_if_valid_data(df: pd.DataFrame) -> bool:
    if df.empty:
        print("No tweets downloaded")
        return False

    #Primary key check
    if df.id.is_unique:
        pass
    else:
        raise Exception("Primary key check violated")

    #check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")


if __name__ == "__main__":
        #extract

    c = twint.Config()
    c.Search = "I rated* /10 #IMDb"
    c.Custom = ["id", "username", "user_id", "tweet", "likes_count", "created_at", "date"]


    # set the date range to be from yesterday morning to yesterday night
    yesterday_morning = datetime.datetime.now() - datetime.timedelta(days=2)
    yesterday_morning = yesterday_morning.strftime("%Y-%m-%d 00:00:00")
    yesterday_night = datetime.datetime.now() - datetime.timedelta(days=2)
    yesterday_night = yesterday_night.strftime("%Y-%m-%d 23:59:59")

    c.Since = yesterday_morning
    c.Until = yesterday_night

    #c.Format = "id: {id}, created_at: {created_at}, tweet: {tweet}, user_id: {user_id}, likes_count: {likes_count}"
    c.Store_object = True

    twint.run.Search(c)


    tweets_list = twint.output.tweets_list
    df = pd.DataFrame(tweets_list)

    #print(df)

    # Validate
    if check_if_valid_data(df):
        print("Data valid, proceed to Load stage")

    # Load

    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('movies.sqlite')
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS tweets(
        id VARCHAR(200),
        username VARCHAR(200),
        user_id VARCHAR(200),
        tweet VARCHAR(200),
        likes_count VARCHAR(200),
        created_at VARCHAR(200),
        date VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (id)
    )
    """


    cursor.execute(sql_query)
    print("Opened database successfully")

    try:
        df.to_sql("tweets", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    conn.close()
    print("Close database successfully")
