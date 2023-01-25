import twint
import pandas as pd
import datetime


c = twint.Config()
c.Search = "I rated* /10 #IMDb"
c.Custom = ["conversation_id", "created_at","tweet", "username", "date", "user_id"]

# set the date range to be from yesterday morning to yesterday night
yesterday_morning = datetime.datetime.now() - datetime.timedelta(days=2)
yesterday_morning = yesterday_morning.strftime("%Y-%m-%d 00:00:00")
yesterday_night = datetime.datetime.now() - datetime.timedelta(days=2)
yesterday_night = yesterday_night.strftime("%Y-%m-%d 23:59:59")

c.Since = yesterday_morning
c.Until = yesterday_night
c.Pandas = True

twint.run.Search(c)

def twint_to_pd(columns):
    return twint.output.panda.Tweets_df[columns]

tweets_df = twint_to_pd(["conversation_id","tweet", "username", "date", "user_id"])
tweets_df.to_csv('tweets.csv', index = False)
