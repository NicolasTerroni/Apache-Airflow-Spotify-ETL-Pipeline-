from airflow import DAG
from airflow.operators.python import get_current_context
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

import datetime
import requests, sqlalchemy, sqlite3
import pandas as pd
from decouple import config

DATABASE_LOC = config("DATABASE_LOC")
USER_ID = config("USER_ID")
TOKEN = config("TOKEN")

def get_data(execution_date):
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}"
    }
    today = execution_date
    yesterday = today - datetime.timedelta(days=1)
    unix_yesterday = int(yesterday.timestamp()) + 1000

    response = requests.get(
        "https://api.spotify.com/v1/me/player/recently-played?limit=50&after={time}".format(time=unix_yesterday),
         headers=headers
         )
    if response.status_code != 200:
        raise Exception(response.json()) 

    data = response.json()
    data_dict = data['items']
    return data_dict


def clean_data(execution_date, data_dict):
    track_names = []
    author_names = []
    datetime_played = []
    date_played = []

    for item in data_dict:
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
    print("Data cleaned")

    # check empty df
    if tracks_df.empty:
        print("No tracks downloaded. Finishing execution")
        quit()
    
    # check unique played_at
    if pd.Series(tracks_df['datetime_played']).is_unique:
        pass
    else:
        raise Exception("Primary key check was violated.")
    
    # check null values
    if tracks_df.isnull().values.any():
        raise Exception("Null value found")
    
    # check all timestamps values are from yesterday
    today = execution_date
    yesterday = today - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0,minute=0,second=0,microsecond=0)
    
    other_dates_in_df = False
    timestamp_values = tracks_df['date_played'].tolist()
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

        yesterday_df = tracks_df[tracks_df['date_played'] ==  str_yesterday]
        print("The tracks that weren't from yesterday were dropped")
    
    if yesterday_df.empty:
        print("No tracks from yesterday downloaded. Finishing execution")
        quit()
    
    print(yesterday_df)
    
    return yesterday_df.to_json()


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



default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime(2022, 6, 1),
    "email": "nsterroni@gmail.com",
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=1)
}

@dag(
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1),
    start_date = days_ago(2),
)
def taskflow_api_etl():
    @task
    def get_data_task():
        context = get_current_context()
        return get_data(context['execution_date'])

    @task
    def clean_data_task(data_dict):
        context = get_current_context()
        return clean_data(context['execution_date'],data_dict)
    
    @task
    def load_data_task(yesteday_data_dict):
        return load_data(pd.read_json(yesteday_data_dict))

    data = get_data_task()
    cleaned_data = clean_data_task(data)
    load_data_task(cleaned_data)


taskflow_api_etl_dag = taskflow_api_etl()
    
