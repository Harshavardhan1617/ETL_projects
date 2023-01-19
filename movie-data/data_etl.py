import sqlalchemy
import pandas as pd
import json
from datetime import datetime
from sqlalchemy.orm import sessionmaker
import sqlite3
import requests

DATABASE_LOCATION = "/mnt/e/ETL_projects/movie-data/database/played_tracks.sqlite"

USER_ID = ""
TOKEN = ""


