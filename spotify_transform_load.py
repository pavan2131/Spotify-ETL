import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO



def album_data(data):
  albums=[]
  for album in data['items']:
       albums.append({"album_id": album['track']['album']['id'],
                      "album_name": album['track']['album']['name'],
                      "total_tracks": album['track']['album']['total_tracks'],
                      "release_date": album['track']['album']['release_date'],
                      "album_url":album['track']['album']['external_urls']['spotify']})

  return albums
  
def song_data(data):
  songs=[]

  for song in data['items']:
      songs.append({'song_id':song['track']['id'],
      'song_name':song['track']['name'],
      'song_duration':song['track']['duration_ms'],
      'song_url':song['track']['external_urls']['spotify'],
      'popularity':song['track']['popularity'],
      'song_added_at':song['added_at'],
      'album_id':song['track']['album']['id'],
      'artist_id':song['track']['album']['artists'][0]['id']})
  
  return songs
  
def artist_data(data):
    artists=[]
    for albums in data['items']:
        for artist in albums['track']['artists']:
            artists.append({'artist_id':artist['id'],'artist_name':artist['name'],'external_url':artist['href']})
    
    return artists

def lambda_handler(event, context):
    s3=boto3.client('s3');
    Bucket = "spoify-etl-pavan"
    key = "raw/to_processed/"
    spotify_data=[]
    spotify_key=[]
    for file in s3.list_objects(Bucket=Bucket,Prefix=key)["Contents"]:
      file_key=file['Key']
      if file_key.split('.')[-1]=='json':
          response=s3.get_object(Bucket=Bucket,Key=file_key)
          content =response['Body']
          jsonObject=json.loads(content.read())
          spotify_data.append(jsonObject)
          spotify_key.append(file_key)
    
    
    for data in spotify_data:
      albums=album_data(data)
      artists=artist_data(data)
      songs=song_data(data)
      
      
    album_df=pd.DataFrame(albums)
    album_df=album_df.drop_duplicates(subset='album_id')
    album_df['release_date']=pd.to_datetime(album_df['release_date'])
    song_df=pd.DataFrame(songs)
    song_df=song_df.drop_duplicates(subset='song_id')
    song_df['song_added_at']=pd.to_datetime(song_df['song_added_at'])
    artist_df=pd.DataFrame(artists)
    artist_df=artist_df.drop_duplicates(subset='artist_id')
    
    
    song_path= "transformed_data/song_data/songs_transformed_"+ str(datetime.now())+".csv"
    song_buffer=StringIO()
    song_df.to_csv(song_buffer,index=False)
    song_content=song_buffer.getvalue()
    s3.put_object(Bucket=Bucket,Key=song_path,Body=song_content)
    
    album_path= "transformed_data/album_data/albums_transformed_"+ str(datetime.now())+".csv"
    album_buffer=StringIO()
    album_df.to_csv(album_buffer,index=False)
    album_content=album_buffer.getvalue()
    s3.put_object(Bucket=Bucket,Key=album_path,Body=album_content)
    
    artist_path= "transformed_data/artist_data/artists_transformed_"+ str(datetime.now())+".csv"
    artist_buffer=StringIO()
    artist_df.to_csv(artist_buffer,index=False)
    artist_content=artist_buffer.getvalue()
    s3.put_object(Bucket=Bucket,Key=artist_path,Body=artist_content)
    
    s3_resource=boto3.resource('s3')
    for key in spotify_key:
      copy_source={
        'Bucket':Bucket,
        'Key':key
      }     
    
      s3_resource.meta.client.copy(copy_source,Bucket,'raw/processed/'+key.split("/")[-1])
      s3_resource.Object(Bucket,key).delete()
      
    
    
      