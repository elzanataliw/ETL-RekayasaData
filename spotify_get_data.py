import spotipy
import pandas as pd
from spotipy.oauth2 import SpotifyOAuth

SPOTIPY_CLIENT_ID = 'c7e4614e974f4ec4aa0a546d38b452bd'
SPOTIPY_CLIENT_SECRET ='c75ecae357c44127b3c1b00f79425881'
SPOTIPY_REDIRECT_URI = 'https://example.com/callback/'

auth_manager = SpotifyOAuth(client_id=SPOTIPY_CLIENT_ID, client_secret=SPOTIPY_CLIENT_SECRET, redirect_uri=SPOTIPY_REDIRECT_URI)
sp = spotipy.Spotify(auth_manager=auth_manager)


def call_playlist(creator, playlist_id, country):
    
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
        playlist_features["poplarity"] = track["track"]["popularity"]
        playlist_features["track_id"] = track["track"]["id"]
        
        # Get audio features
        audio_features = sp.audio_features(playlist_features["track_id"])[0]
        for feature in playlist_features_list[5:]:
            playlist_features[feature] = audio_features[feature]
        
        # Concat the dfs
        track_df = pd.DataFrame(playlist_features, index = [0])
        playlist_df = pd.concat([playlist_df, track_df], ignore_index = True)

    #Step 3
        
    return playlist_df
print(call_playlist("spotify","37i9dQZEVXbMDoHDwVN2tF","ID"))
# playlist = sp.user_playlist_tracks('spotify', '37i9dQZEVXbMDoHDwVN2tF', limit=5, market='ID')["items"]
# print(playlist)