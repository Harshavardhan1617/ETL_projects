import sqlalchemy
import pandas as pd
import sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import sqlite3

DATABASE_LOCATION = "/mnt/e/ETL_projects/spotify-etl/played_tracks.sqlite"
USER_ID = ""
TOKEN = ""

def check_data_validity(df: pd.DataFrame) -> bool:
    #check for empty dataframe
    if df.empty:
        print("No songs downloaded, finishing execution")
        return False

    #primary key check
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("primary key is violated")

    #check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

    #check if timestamps are of yesterdays
    yesyerday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
            raise Exception("Atleast one of the songs returned doesnot have yesterday's timestamp")

    return True

if __name__ == "__main__":

    #Extract

    headers = {
            "Accept" : "application/json",
            "Content-Type" : "application/json",
            "Authorization" : "Bearer {token}".format(token=TOKEN)
    }

    #convert time to unix timestamp in milliseconds
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    #Download all songs i listened to in last 24 hours

r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = headers)

data = r.json()

song_name = []
artist_names = []
played_at_list = []
timestamps = []

#extracting only relevant data from json object
for song in data["items"]:
    song_names.append(song["track"]["name"])
    artist_names.append(song["track"]["album"])
    played_at_list.append(song["pkayed_at"])
    timestamps.append(song["played_at"][0:10])

#preparing dict to turn data into dataframe
song_dict = {
        "song_name" : artist_names,
        "artist_name" : artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
}

song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])

#validate

