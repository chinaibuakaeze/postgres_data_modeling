import json, os, glob, psycopg2
import pandas as pd


def get_listof_files(file_path):
    #Get list of all JSON files in a given directory
    files_list = []
    dir_content = os.walk(file_path)
    for root, dirs, files in dir_content:
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files:
            files_list.append(os.path.abspath(f))

    return files_list

def escape_characters(song_row):
    #escape characters with double quotes and apostrophes
    value = song_row.split(', ')
    new_str = ""
    for v in value:
        if v.startswith('"') and v.endswith('"'):
            v = v.replace("'", "''")
            v = v.replace('"', "'")
            new_str = new_str + ', ' + v
        else:
            if v == value[0]:
                new_str = new_str + v
            else:
                new_str = new_str + ', ' + v
    return new_str


def read_song_files(files_list, cur):
    #Read song data and insert into artist and songs database
    for file in files_list:
        reader = open(file)
        rows= json.load(reader)
        #INSERT statement for songs_table
        song_row = f"INSERT INTO songs(song_id, title, artist_id, year, duration) VALUES{rows['song_id'], rows['title'], rows['artist_id'], rows['year'], rows['duration']}"
        #INSERT into songs_table
        try:
            cur.execute(song_row)
        #Escape characters with double quotes and apostrophes
        except psycopg2.Error as e:
            song_str = escape_characters(song_row)
            cur.execute(song_str)
        artist_row = f"INSERT INTO artists(artist_id, name, location, latitude, longitude) VALUES{rows['artist_id'], rows['artist_name'], rows['artist_location'], rows['artist_latitude'], rows['artist_longitude']}"
        artist_row = artist_row.replace("None", '0')
        try:
            cur.execute(artist_row)
        except psycopg2.Error as e:
            try:
                artist_str = escape_characters(artist_row)
                cur.execute(artist_str)
            except psycopg2.Error as e:
                pass

def read_log_files(files_list, cur):
    count = 0
    for file in files_list:
        df = pd.read_json(file, lines=True)
        times = (pd.to_datetime(df.loc[count]['ts'], unit='ms'))
        time_row = f"INSERT INTO time(start_time, hour, day, week, month, year, weekday) \
            VALUES{str(times), times.hour, times.day, times.week, times.month, times.year, times.day_name()}"
        cur.execute(time_row)
        if df.loc[count]['userId'] == '':
            del df
        else:
        #Format sql string for inserting to user table
            user_row = f"INSERT INTO users(user_id, first_name, last_name, gender, level) \
            VALUES{df.loc[count]['userId'], df.loc[count]['firstName'], df.loc[count]['lastName'], df.loc[count]['gender'], df.loc[count]['level']}"
            #Get artist_id and song_id
            select_str = f"SELECT songs.song_id, artists.artist_id FROM songs JOIN artists USING(artist_id) WHERE songs.title='{df.loc[count]['song']}' \
            AND artists.name = '{df.loc[count]['artist']}' AND songs.duration = {df.loc[count]['length']}"
            count += 1
            try:
                cur.execute(user_row)
            except psycopg2.Error as e:
                pass
            
            try:
                cur.execute(select_str)
                results = cur.fetchone()
            except psycopg2.Error as e:
                pass 
            if results:
                songid, artistid = results
            else:
                songid, artistid = 'None', 'None'
            #Insert into songplay table
            songplays= f"INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
                VALUES{str(times), df.loc[count]['userId'], df.loc[count]['level'], songid, artistid, df.loc[count]['sessionId'], df.loc[count]['location'], df.loc[count]['userAgent']}"
            try:
                cur.execute(songplays)
            except psycopg2.Error as e:
                pass
        
        

    
  
    
            
        

    

        
            

        
     

  