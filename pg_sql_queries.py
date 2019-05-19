# DROP TABLES

staging_events_table_drop = "drop table if exists s_songplay_event;"
staging_songs_table_drop = "drop table if exists s_song;"
songplay_table_drop = "drop table if exists f_songplay;"
user_table_drop = "drop table if exists d_user;"
song_table_drop = "drop table if exists d_song;"
artist_table_drop = "drop table if exists d_artist;"
time_table_drop = "drop table if exists d_time;"

# CREATE TABLES

# songplay_id is always 64 characters (hex digest of sha256 hash)
staging_events_table_create= ("""
create table if not exists s_songplay_event (
    songplay_id varchar(64),
    artist text,
    auth text,
    first_name text,
    item_in_session integer,
    last_name text,
    gender char(1),
    length decimal,
    level text,
    location text,
    method text,
    page text,
    registration decimal,
    session_id integer,
    song text,
    status smallint,
    timestamp text,
    year smallint,
    week smallint,
    month smallint,
    day smallint,
    hour smallint,
    weekday smallint,
    user_agent text,
    user_id text
);
""")

staging_songs_table_create = ("""
create table if not exists s_song (
    artist_id varchar(18),
    artist_location varchar(96),
    artist_latitude decimal,
    artist_longitude decimal,
    artist_name varchar(256),
    duration decimal,
    num_songs smallint,
    song_id varchar(18),
    title varchar(256),
    year smallint
);
""")

songplay_table_create = ("""
create table if not exists f_songplay (
    songplay_id text primary key not null,
    start_time text not null references d_time(start_time),
    user_id text not null references d_user(user_id),
    artist_id text references d_artist(artist_id),
    song_id text references d_song(song_id),
    level text,
    session_id text,
    location text,
    user_agent text
);
""")

user_table_create = ("""
create table if not exists d_user (
    user_id text primary key not null,
    first_name text,
    last_name text,
    gender text,
    level text
);
""")

song_table_create = ("""
create table if not exists d_song (
    song_id text primary key not null,
    title text,
    artist_id text,
    year text,
    duration text
);
create index idx_song_title on d_song(title);
""")

artist_table_create = ("""
create table if not exists d_artist (
    artist_id text primary key not null,
    name text,
    location text,
    latitude text,
    longitude text
);
create index idx_artist_name on d_artist(name);
""")

time_table_create = ("""
create table if not exists d_time (
    start_time text primary key not null,
    hour text,
    day text,
    week text,
    month text,
    year text,
    weekday text
);
""")

# STAGING TABLES

staging_events_copy = ("""
insert into s_songplay_event (
    songplay_id,
    artist,
    auth,
    first_name,
    item_in_session,
    last_name,
    gender,
    length,
    level,
    location,
    method,
    page,
    registration,
    session_id,
    song,
    status,
    timestamp,
    year,
    week,
    month,
    day,
    hour,
    weekday,
    user_agent,
    user_id
    ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    on conflict do nothing;
""")

staging_songs_copy = ("""
insert into s_song (
    artist_id,
    artist_latitude,
    artist_location,
    artist_longitude,
    artist_name,
    duration,
    num_songs,
    song_id,
    title,
    year
    ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    on conflict do nothing;
""")

# FINAL TABLES

songplay_table_insert = ("""
insert into f_songplay (songplay_id, start_time, user_id, artist_id, song_id, level, session_id, location, user_agent)
select
    songplay_id,
    timestamp,
    user_id,
    (select artist_id from d_artist d where d.name = artist limit 1) as artist_id,
    (select song_id from d_song d where d.artist_id = artist_id and d.title = song limit 1),
    level,
    session_id,
    location,
    user_agent
from s_songplay_event;
""")

user_table_insert = ("""
insert into d_user (user_id, first_name, last_name, gender, level)
select distinct(user_id), first_name, last_name, gender, level from s_songplay_event
on conflict do nothing;
""")

song_table_insert = ("""
insert into d_song (song_id, title, artist_id, year, duration)
select distinct(song_id), title, artist_id, year, duration from s_song
on conflict do nothing;
""")

artist_table_insert = ("""
insert into d_artist (artist_id, name, location, latitude, longitude)
select distinct(artist_id), artist_name, artist_location, artist_latitude, artist_longitude from s_song
on conflict do nothing;
""")

time_table_insert = ("""
insert into d_time (start_time, year, week, month, day, hour, weekday)
select distinct(timestamp), year, week, month, day, hour, weekday from s_songplay_event
on conflict do nothing;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
