import psycopg2

def drop_tables(cur):
    cur.execute("DROP TABLE IF EXISTS songs, users, artists, time, songplays")

def create_songs_table(cur):
    #Create dimension table for songs
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS songs(song_id VARCHAR(255) PRIMARY KEY, title VARCHAR(255), artist_id VARCHAR(255), \
            year INT, duration FLOAT)")
    except psycopg2.Error as e:
        print(e)

def create_users_table(cur):
    #Create dimension table for users
    try:
        cur.execute("CREATE TABLE  IF NOT EXISTS users(user_id INT PRIMARY KEY, first_name VARCHAR(255), last_name VARCHAR(255), \
            gender TEXT, level TEXT)")
    except psycopg2.Error as e:
        print(e)

def create_artists_table(cur):
    #Create dimension table for artists
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS artists(artist_id VARCHAR(255) PRIMARY KEY, name VARCHAR(255), location VARCHAR(255), \
        latitude FLOAT, longitude FLOAT)")
    except psycopg2.Error as e:
        print(e)

def create_time_table(cur):
    #Create dimension table for time
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS time(start_time VARCHAR(255) PRIMARY KEY, hour INT, day INT, week INT, \
            month INT, year INT, weekday VARCHAR(50))")
    except psycopg2.Error as e:
        print(e)

def create_songplays(cur):
    #Create facts table for songplays
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS songplays(songplay_id SERIAL PRIMARY KEY, start_time TIME, user_id INT, level TEXT, song_id VARCHAR(255), \
            artist_id VARCHAR(255), session_id INT, location VARCHAR(255), user_agent VARCHAR(255))")
    except psycopg2.Error as e:
        print(e)

