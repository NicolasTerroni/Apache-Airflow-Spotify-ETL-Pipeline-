import requests, sqlalchemy, sqlite3
import datetime
import pandas as pd
from decouple import config
from cfg import *

DATABASE_LOC = config("DATABASE_LOC")
USER_ID = config("USER_ID")
TOKEN = config("TOKEN")


def load_data(df: pd.DataFrame) -> bool:
    engine = sqlalchemy.create_engine(DATABASE_LOC)
    conn = sqlite3.connect('played_tracks.sqlite')
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS played_tracks(
        track_name VARCHAR(200),
        author_name VARCHAR(200),
        datetime_played DATETIME(200),
        date_played DATE(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (datetime_played)
    )
    """
    cursor.execute(sql_query)

    try:
        df.to_sql("played_tracks", engine, index=False, if_exists="append")
        print("Data loaded succesfully.")
    except:
        print("Data already exists in the database")
    finally:
        conn.close()


def drop_other_dates_data(df: pd.DataFrame) -> bool:
    # check all timestamps values are from yesterday
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0,minute=0,second=0,microsecond=0)
    
    other_dates_in_df = False
    timestamp_values = df['date_played'].tolist()
    for timestamp in timestamp_values:
        if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
            other_dates_in_df = True
    
    if other_dates_in_df:
        year = str(yesterday.year)
        month = str(yesterday.month)
        if len(month) == 1:
            month = "0"+month
        day = str(yesterday.day)
        if len(day) == 1:
            day = "0"+day

        str_yesterday = f"{year}-{month}-{day}"

        yesterday_df = df[df['date_played'] ==  str_yesterday]
        print("The tracks that weren't from yesterday were dropped")
    
    if df.empty:
        print("No tracks from yesterday downloaded. Finishing execution")
        return False
 
    return yesterday_df


def check_valid_data(df: pd.DataFrame) -> bool:

    # check empty df
    if df.empty:
        print("No tracks downloaded. Finishing execution")
        return False
    
    # check unique played_at
    if pd.Series(df['datetime_played']).is_unique:
        pass
    else:
        raise Exception("Primary key check was violated.")
    
    # check null values
    if df.isnull().values.any():
        raise Exception("Null value found")
    return True


def clean_data(data):
    track_names = []
    author_names = []
    datetime_played = []
    date_played = []

    for item in data:
        track_names.append(item['track']['name'])
        author_names.append(item['track']['album']['artists'][0]['name'])
        datetime_played.append(item['played_at'])
        date_played.append(item['played_at'][:10])

    tracks_dict = {
        "track_name": track_names,
        "author_name": author_names,
        "datetime_played": datetime_played,
        "date_played": date_played
    }
    tracks_df = pd.DataFrame(
        tracks_dict, 
        columns=["track_name","author_name","datetime_played","date_played"]
    )
    return tracks_df


def get_data():
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}"
    }
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    unix_yesterday = int(yesterday.timestamp()) + 1000

    response = requests.get(
        "https://api.spotify.com/v1/me/player/recently-played?limit=50&after={time}".format(time=unix_yesterday),
         headers=headers
         )
    if response.status_code != 200:
        raise Exception(response.json()) 

    data = response.json()
    return data


def run_whole_etl():
    """
    Run whole ETL pipeline.
    """
    # extract
    response = get_data()
    raw_data = response['items'] # we could store the raw data in a data lake if we want to
    
    # transform / validate
    tracks_df = clean_data(raw_data)
    if check_valid_data(tracks_df):
        print("Data is valid.")
    yesterday_tracks_df = drop_other_dates_data(tracks_df)

    # load
    load_data(yesterday_tracks_df)


if __name__ == "__main__":
    run_whole_etl()


