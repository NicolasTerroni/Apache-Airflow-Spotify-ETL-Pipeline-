import requests, sqlalchemy, json, csv, sqlite3
import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd

DATABASE_LOC = "sqlite:///played_tracks.sqlite"
USER_ID = "nicot09"
TOKEN = "BQCEmzi9YuG6fU2Ys2sOnTSgMLwCsTfgtghURMK7-0a01WUBJKGzIrdCx_Ey0KjwZO5EaOWIrNzWt5feU9-nLlFENb2TQmrjhqTu6QrCsKabIvd3mkDQLJ_Xx8wxbZeqt5MFW-uKJg26dw_cjMoe9oOAYXIVE0kLV_GzhGFV"

def check_valid_data(df: pd.DataFrame) -> bool:

    # check empty df
    if df.empty:
        print("No tracks downloaded. Finishing execution")
        return False
    
    # check unique played_at
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary key check was violated.")
    
    # check null values
    if df.isnull().values.any():
        raise Exception("Null value found")

    # check all timestamps values are from yesterday
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0,minute=0,second=0,microsecond=0)
    
    timestamp_values = df['timestamp'].tolist()
    for timestamp in timestamp_values:
        if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
            raise Exception("At least one of the tracks is not from the last 24hs")
    
    return True


def get_data():
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}"
    }
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    unix_timestamp_str = int(yesterday.timestamp()) + 1000

    response = requests.get(
        f"https://api.spotify.com/v1/me/player/recently-played?limit=50&after={unix_timestamp_str}",
         headers=headers
         )
    data = response.json()

    track_names = []
    author_names = []
    played_at_lists = []
    timestamps = []

    for item in data['items']:
        track_names.append(item['track']['name'])
        author_names.append(item['track']['album']['artists'][0]['name'])
        played_at_lists.append(item['played_at'])
        timestamps.append(item['played_at'][0:10])

    tracks_dict = {
        "track_name": track_names,
        "author_name": author_names,
        "played_at": played_at_lists,
        "timestamp": timestamps
    }
    tracks_df = pd.DataFrame()
    tracks_df = pd.DataFrame(
        tracks_dict, 
        columns=["track_name","author_name","played_at","timestamp"]
    )
    return tracks_df


if __name__ == "__main__":
    yesterday_tracks_df = get_data()
    
    if check_valid_data(yesterday_tracks_df):
        print("ok")
    



#import pdb; pdb.set_trace()
