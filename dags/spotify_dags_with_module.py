from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import spotipy
import pandas as pd
from spotipy.oauth2 import SpotifyOAuth, SpotifyClientCredentials
import psycopg2
from sqlalchemy import create_engine



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 29),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def get_playlist(creator, playlist_id, country):
    SPOTIPY_CLIENT_ID = 'a4ce8902dd7348638f9a7e2b141a7a78'
    SPOTIPY_CLIENT_SECRET ='a7109ef5087f44a09d67be493c67de78'
    SPOTIPY_REDIRECT_URI = 'https://example.com/callback/'

    auth_manager = SpotifyClientCredentials(client_id=SPOTIPY_CLIENT_ID, client_secret=SPOTIPY_CLIENT_SECRET)
    sp = spotipy.Spotify(auth_manager=auth_manager)
    
   #step1

    playlist_features_list = ["artist","album","track_name","popularity",  "track_id","danceability","energy","key","loudness","mode", "speechiness","instrumentalness","liveness","valence","tempo", "duration_ms","time_signature"]
    
    playlist_df = pd.DataFrame(columns = playlist_features_list)
    
    #step2
    
    playlist = sp.user_playlist_tracks(creator, playlist_id, market=country)["items"]
    for track in playlist:
        # Create empty dict
        playlist_features = {}
        # Get metadata
        playlist_features["artist"] = track["track"]["album"]["artists"][0]["name"]
        playlist_features["album"] = track["track"]["album"]["name"]
        playlist_features["track_name"] = track["track"]["name"]
        playlist_features["popularity"] = track["track"]["popularity"]
        playlist_features["track_id"] = track["track"]["id"]
        
        # Get audio features
        audio_features = sp.audio_features(playlist_features["track_id"])[0]
        for feature in playlist_features_list[5:]:
            playlist_features[feature] = audio_features[feature]
        
        # Concat the dfs
        track_df = pd.DataFrame(playlist_features, index = [0])
        playlist_df = pd.concat([playlist_df, track_df], ignore_index = True)

    #Step 3
    conn_string = 'postgres://postgres:hujanbadai@host.docker.internal/spotipi'
  
    db = create_engine(conn_string)
    conn = db.connect()
    playlist_df.to_csv("/opt/airflow/data/spotify_sadd.csv")

    playlist_df.to_sql('spotipi', con=conn, if_exists='append',
          index=False)
    conn = psycopg2.connect(conn_string
                        )
    conn.autocommit = True
    conn.close()
    return playlist_df


with DAG(
    'spotify_dags_v10',
    default_args=default_args,
    description='Our first DAG with ETL process!',
    schedule_interval='@once'
) as dag:

    run_etl = PythonOperator(
        task_id='whole_spotify_etl',
        python_callable=get_playlist,
        op_kwargs={
            'creator':"spotify",
            'playlist_id':"37i9dQZF1DXbrUpGvoi3TS",
            'country': "ID"
        },
        dag=dag
    )

    run_etl
    