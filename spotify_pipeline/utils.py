import requests
import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
from datetime import datetime as dt, timedelta
from dotenv import load_dotenv

import os

# load the envs
load_dotenv()

# spotify credentials
sptoify_token = os.getenv('SPOTIFY_TOKEN')
sptoify_url = os.getenv('SPOTIFY_URL')

# database credentials
username = os.getenv('DBUSER')
password = os.getenv('DBPASS')
host = os.getenv('DBHOST')
db = os.getenv('DB')


def covert_to_dataframe(songs_info_list: list) -> pd.DataFrame:

    if len(songs_info_list) == 0:
        raise Exception('No songs were listen from yesterday till now')

    tracks = []
    artists = []
    albums = []
    played_at = []

    for song in songs_info_list:
        tracks.append(song['track']['name'])
        artists.append(song['track']['artists'][0]['name'])
        albums.append(song['track']['album']['name'])
        played_at.append(dt.strptime(song['played_at'], '%Y-%m-%dT%H:%M:%S.%fZ'))

    return pd.DataFrame({
        'track': tracks,
        'artist': artists,
        'album': albums,
        'played_at': played_at,
    })

def remove_missing_data(df: DataFrame) -> DataFrame:
    return df.dropna()

def remove_duplicate_data(df: DataFrame) -> DataFrame:
    return df.drop_duplicates(subset='played_at')

def recently_played_songs_json():

    headers = {
        'Authorization': f'Bearer {sptoify_token}',
        'Content-Type': 'application/json; charset=utf-8',
    }

    yesterday = int((dt.today() - timedelta(1)).timestamp() * 1000) # unix timestamp
        
    params = {'after': yesterday}

    res = requests.get(sptoify_url, headers=headers, params=params)

    return res.json()

def connect_to_mysql() -> Connection:

    url = f'mysql://{username}:{password}@{host}?charset=utf8'

    # create the engine
    engine = create_engine(url = url)

    # create database and connect to that database
    engine.execute(f'create database if not exists {db}')
    engine.execute(f'use {db}')

    return engine.connect()


def load_dataframe_to_mysql(df: DataFrame,con: Connection, table_name: str):
    df.to_sql(
        name=table_name, 
        schema=db,
        con=con, 
        if_exists='append', 
        index=False,
    )
