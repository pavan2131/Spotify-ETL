import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
   client_id=os.environ.get('client_id')
   client_secret=os.environ.get('client_secret')
   client_credentials_manager=SpotifyClientCredentials(client_id=client_id,client_secret=client_secret)
   sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
   
   play_list_link= "https://open.spotify.com/playlist/37i9dQZEVXbMDoHDwVN2tF"
   playlist_id=play_list_link.split("/")[-1]
   
   spotify_data = sp.playlist_tracks(playlist_id)
   client = boto3.client('s3')
   
   file_name= "spotify_raw_"+ str(datetime.now())+".json"
   client.put_object(
      Bucket= "spoify-etl-pavan",
      Key= "raw/to_processed/"+file_name,
      Body=json.dumps(spotify_data)
      
      )