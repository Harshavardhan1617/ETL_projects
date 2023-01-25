import sqlalchemy
import pandas as pd
import sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import sqlite3

DATABASE_LOCATION = "/mnt/e/ETL_projects/spotify-etl/played_tracks.sqlite"
USER_ID = "harsha"
TOKEN = "BQBgFO-vKlZOC0Nf2DMmYrxNAtB1FBkKj79GZTAYib4stHDtyIPj8JZ4s4AKhNGy-9p19aleSh-ejoNG3CWNlC_7Y4OINh33rn-GwgZpZsCimL2oKOhgur7HLpWCAL2dJ_x0hHNBfKtgjaoWx0JWBykCCtgGUCdTLzNRd7XLsW37hJMnd4xPMijROWAHFbOFvPkA9A"

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

print(data)
