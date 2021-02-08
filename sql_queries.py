import configparser


# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get('S3', 'LOG_DATA')
SONG_DATA = config.get('S3', 'SONG_DATA')
ARN = config.get('IAM_ROLE', 'ARN')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
REGION = config.get('GEO', 'REGION')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events
    (
      artist TEXT, 
      auth TEXT, 
      firstName TEXT, 
      gender TEXT, 
      ItemInSession TEXT, 
      lastName TEXT, 
      length FLOAT, 
      level TEXT, 
      location TEXT, 
      method TEXT, 
      page TEXT, 
      registration TEXT, 
      sessionId INT, 
      song TEXT, 
      status INT, 
      ts TIMESTAMP, 
      userAgent TEXT, 
      userId INT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs 
    (
      song_id TEXT, 
      artist_id TEXT, 
      artist_latitude FLOAT, 
      artist_location TEXT, 
      artist_longitude FLOAT, 
      artist_name TEXT, 
      duration FLOAT, 
      num_songs INT, 
      title TEXT, 
      year INT
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
    (
      songplay_id INT IDENTITY(0,1) PRIMARY KEY, 
      start_time TIMESTAMP NOT NULL REFERENCES time(start_time) SORTKEY, 
      user_id INT NOT NULL REFERENCES users(user_id), 
      level TEXT NOT NULL, 
      song_id TEXT NOT NULL REFERENCES songs(song_id), 
      artist_id TEXT NOT NULL REFERENCES artists(artist_id) DISTKEY, 
      session_id INT NOT NULL, 
      location TEXT NOT NULL, 
      user_agent TEXT NOT NULL
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (
      user_id INT PRIMARY KEY SORTKEY, 
      first_name TEXT NOT NULL, 
      last_name TEXT NOT NULL, 
      gender TEXT NOT NULL, 
      level TEXT NOT NULL
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
    (
      song_id TEXT PRIMARY KEY SORTKEY, 
      title TEXT NOT NULL, 
      artist_id TEXT NOT NULL, 
      year INT NOT NULL, 
      duration FLOAT NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
    (
      artist_id TEXT PRIMARY KEY DISTKEY, 
      name TEXT NOT NULL, 
      location TEXT NOT NULL, 
      lattitude FLOAT NOT NULL, 
      longitude FLOAT NOT NULL
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
      start_time timestamp PRIMARY KEY SORTKEY, 
      hour INT NOT NULL, 
      day INT NOT NULL, 
      week INT NOT NULL, 
      month INT NOT NULL, 
      year INT NOT NULL, 
      weekday INT NOT NULL
    );
""")

# STAGING TABLES

staging_events_copy = (f"""
COPY staging_events FROM {LOG_DATA} iam_role {ARN} region {REGION} FORMAT AS JSON {LOG_JSONPATH} timeformat 'epochmillisecs';
""")

staging_songs_copy = (f"""
COPY staging_songs FROM {SONG_DATA} iam_role {ARN} region {REGION} FORMAT AS JSON 'auto';
""")

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
  SELECT DISTINCT se.ts, se.userId, se.level, ss.song_id, ss.artist_id, se.sessionId, se.location, se.userAgent
  FROM staging_events se 
  INNER JOIN staging_songs ss 
  ON (se.song = ss.title AND se.artist = ss.artist_name)
  WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
  SELECT DISTINCT se.userId, se.firstName, se.lastName, se.gender, se.level
  FROM staging_events se
  WHERE se.page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
  SELECT DISTINCT ss.song_id, ss.title, ss.artist_id, ss.year, ss.duration
  FROM staging_songs ss;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, lattitude, longitude)
  SELECT DISTINCT ss.artist_id, 
    ss.artist_name, 
    CASE WHEN ss.artist_location IS NULL THEN 'N/A' ELSE ss.artist_location END, 
    CASE WHEN ss.artist_latitude IS NULL THEN 0.0 ELSE ss.artist_latitude END, 
    CASE WHEN ss.artist_longitude IS NULL THEN 0.0 ELSE ss.artist_longitude END
  FROM staging_songs ss
  WHERE ss.artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
  SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
    EXTRACT(hour FROM start_time) AS hour,
    EXTRACT(day FROM start_time) AS day,
    EXTRACT(week FROM start_time) AS week,
    EXTRACT(month FROM start_time) AS month,
    EXTRACT(year FROM start_time) AS year,
    EXTRACT(week FROM start_time) AS weekday
  FROM songplays AS se
  WHERE se.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]