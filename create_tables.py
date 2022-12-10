import psycopg2
#Connect to sparkify database
try:
    conn = psycopg2.connect("dbname=sparkify_db user=china password=test")
except psycopg2.Error as e:
    print(e)

cur = conn.cursor()
conn.set_session(autocommit=True)


def create_songs_table():
    #Create dimension table for songs
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS songs(song_id VARCHAR(255) PRIMARY KEY, title VARCHAR(255), artist_id VARCHAR(255), \
            year INT, duration NUMERIC)")
    except psycopg2.Error as e:
        print(e)

def create_users_table():
    #Create dimension table for users
    try:
        cur.execute("CREATE TABLE  IF NOT EXISTS users(user_id INT PRIMARY KEY, first_name VARCHAR(255), last_name VARCHAR(255), \
            gender TEXT, level TEXT)")
    except psycopg2.Error as e:
        print(e)

def create_artists_table():
    #Create dimension table for artists
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS artists(artist_id VARCHAR(255) PRIMARY KEY, name VARCHAR(255), location VARCHAR(255), \
        latitude NUMERIC, longitude NUMERIC)")
    except psycopg2.Error as e:
        print(e)

def create_time_table():
    #Create dimension table for time
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS time(time_id INT PRIMARY KEY, start_time TIME, hour TIME, day DATE, week TEXT, \
            month DATE, year DATE, weekday VARCHAR(50))")
    except psycopg2.Error as e:
        print(e)

def create_songplays():
    #Create facts table for songplays
    try:
        cur.execute("CREATE TABLE songplays(songplay_id VARCHAR(255) PRIMARY KEY, start_time TIME, user_id INT, level_song TEXT, \
            artist_id VARCHAR(255), session_id INT, location VARCHAR(255), user_agent VARCHAR(255))")
    except psycopg2.Error as e:
        print(e)


def main():
    create_songs_table()
    create_users_table()
    create_artists_table()
    create_time_table()
    create_songplays()
main()