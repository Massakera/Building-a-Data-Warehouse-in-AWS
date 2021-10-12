import configparser


import configparser

# CONFIG

config=configparser.ConfigParser()
config.read('dwh.cfg')
SONG_DATA=config.get('S3','SONG_DATA')
LOG_DATA=config.get('S3','LOG_DATA')
LOG_JSONPATH=config.get('S3','LOG_JSONPATH')
IAM=config.get('IAM_ROLE','ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_log_events_data;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_data;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"



# CREATE TABLES

staging_events_table_create="""
CREATE TABLE staging_log_events_data
(
event_id int IDENTITY(0,1),
artist varchar,
auth varchar,
firstName varchar,
gender varchar,  
itemInSession varchar,
lastName varchar,
length varchar, 
level varchar,
location varchar, 
method varchar,
page varchar,
registration varchar,
sessionId varchar, 
song varchar,
status bigint,
ts bigint,
userAgent text,
userId varchar,
PRIMARY KEY (event_id)
);
"""


staging_songs_table_create="""
CREATE TABLE staging_songs_data
(
song_id varchar(100),
artist_id varchar(255),
artist_latitude text,
artist_location text,
artist_longitude text,  
artist_name varchar(255),
duration float,
num_songs int, 
title text,
year int,
primary key (song_id)
)
"""


songplay_table_create="""
CREATE TABLE songplays
(
    songplay_id int identity(0,1),
    start_time timestamp not null sortkey,
    user_id text,
    level varchar(20) not null,
    song_id varchar(255),
    artist_id varchar(255) not null,
    session_id varchar(20) not null,
    location text,
    user_agent text not null,
    primary key (songplay_id)
)
diststyle all;
"""



time_table_create="""
CREATE TABLE time
(
    start_time timestamp not null sortkey,
    hour int not null,
    day int not null,
    week int not null,
    month int not null,
    year int not null,
    weekday int not null,
    primary key (start_time)
)
diststyle all;
"""


artist_table_create="""
CREATE TABLE artists
(
    artist_id text sortkey,
    name varchar(255) not null,
    location text,
    lattitude text,
    longitude text,
    primary key (artist_id)
)
diststyle all;
"""


song_table_create="""
CREATE TABLE songs
(
    song_id varchar(255) sortkey,
    title text,
    artist_id varchar(255),
    year int not null,
    duration float,
    primary key (song_id)
)
diststyle all;
"""
user_table_create="""
CREATE TABLE users
(
    user_id varchar(255) sortkey,
    first_name varchar(120),
    last_name varchar(120),
    gender varchar(10) not null,
    level varchar(10) not null,
    primary key (user_id)
)
diststyle all;
"""


# STAGING TABLES

staging_songs_table_copy="""
COPY staging_songs_data(song_id,artist_id,artist_latitude,artist_location,artist_longitude,artist_name,
duration,num_songs,title,year)
from {}
credentials 'aws_iam_role={}'
json 'auto'
""".format(SONG_DATA,IAM)

staging_events_table_copy="""
COPY staging_log_events_data
(event_id,artist,auth,firstName,gender,itemInSession,lastName,length,level,location,method,page,registration,sessionId,song,status,ts,userAgent,userId)
from {}
credentials 'aws_iam_role={}'
json {}
""".format(LOG_DATA,IAM,LOG_JSONPATH)

# FINAL TABLES

time_table_insert="""
INSERT INTO time (                  
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday)
    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'        AS start_time,
            EXTRACT(hour FROM start_time)    AS hour,
            EXTRACT(day FROM start_time)     AS day,
            EXTRACT(week FROM start_time)    AS week,
            EXTRACT(month FROM start_time)   AS month,
            EXTRACT(year FROM start_time)    AS year,
            EXTRACT(week FROM start_time)    AS weekday
    FROM    staging_log_events_data AS se
    WHERE se.page = 'NextSong';
"""


user_table_insert="""INSERT into users(user_id,first_name,last_name,gender,level)
SELECT DISTINCT userId,firstName,lastName,gender,level 
from staging_log_events_data as se where se.page='NextSong';
"""

artist_table_insert="""INSERT into artists(artist_id,name,location,lattitude,longitude) 
SELECT DISTINCT
artist_id,artist_name,artist_location,artist_latitude,artist_longitude
from staging_songs_data;
"""

song_table_insert="""INSERT into songs(song_id,title,artist_id,year,duration) 
SELECT DISTINCT
song_id,title,artist_id,year,duration from staging_songs_data;
"""

songplay_table_insert="""INSERT into songplays(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'  AS start_time,
userId,level,s.song_id,s.artist_id,sessionId,location,userAgent 
from staging_log_events_data se join staging_songs_data s on (se.song=s.title AND se.artist=s.artist_name) and se.page='NextSong';
"""

# QUERY LISTS

drop_table_queries=[staging_events_table_drop,staging_songs_table_drop,songplay_table_drop,user_table_drop
,song_table_drop,artist_table_drop,time_table_drop]
create_table_queries=[staging_events_table_create,staging_songs_table_create,songplay_table_create,user_table_create
,song_table_create,artist_table_create,time_table_create]
copy_table_queries=[staging_songs_table_copy,staging_events_table_copy]
insert_table_queries=[time_table_insert,song_table_insert,artist_table_insert,songplay_table_insert]

