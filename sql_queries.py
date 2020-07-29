import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "drop table if exists staging_songs;"
songplay_table_drop = "drop table if exists songplay;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists time;"

# CREATE TABLES

staging_events_table_create= ("""
    create table staging_events (
        artistName varchar(256),
        auth varchar(64),
        userFirstName varchar(64),
        gender char(1),
        itemInSession int,
        userLastName varchar(64),
        length float,
        level varchar(32),
        location varchar(256),
        method varchar(8),
        page varchar(32),
        registration float,
        sessionId int,
        songTitle varchar(256),
        status int,
        ts timestamp,
        userAgent varchar(256),
        userId int
    )
""")

staging_songs_table_create = ("""
    create table staging_songs (
        num_songs int,
        artist_id varchar(64),
        artist_latitude float,
        artist_longitude float,
        artist_location varchar(256),
        artist_name varchar(256),
        song_id varchar(64),
        title varchar(256),
        duration float,
        year int
    )
""")

songplay_table_create = ("""
    create table songplay (
        songplayId int identity(1,1) primary key,
        timeId timestamp NOT NULL,
        userId int NOT NULL,
        level varchar(32),
        songId varchar(64) NOT NULL,
        artistId varchar(64) NOT NULL,
        sessionId int,
        location varchar(256),
        userAgent varchar(256),
        FOREIGN KEY (timeId) REFERENCES time(timeId),
        FOREIGN KEY (userId) REFERENCES users(userId),
        FOREIGN KEY (songId) REFERENCES songs(songId),
        FOREIGN KEY (artistId) REFERENCES artists(artistId)
    )
""")

user_table_create = ("""
    create table users (
        userId int primary key,
        userFirstName varchar(64),
        userLastName varchar(64),
        gender char(1),
        level varchar(32)
    )
""")

song_table_create = ("""
    create table songs (
        songId varchar(64) primary key unique,
        songTitle varchar(256),
        artistId varchar(64),
        year int,
        duration float
    )
""")

artist_table_create = ("""
    create table artists (
        artistId varchar(64) primary key,
        artistName varchar(256),
        artistLocation varchar(256),
        artistLatitude float,
        artistLongitude float
    )
""")

time_table_create = ("""
    create table time (
        timeId timestamp primary key,
        hour smallint,
        day smallint,
        week smallint,
        month smallint,
        year int,
        weekday smallint
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    compupdate off
    timeformat as 'epochmillisecs'
    format as json {}
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['DB_ROLE_ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    compupdate off
    json 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['DB_ROLE_ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    insert into songplay (
        timeId,
        userId,
        level,
        songId,
        artistId,
        sessionId,
        location,
        userAgent
    )
    select distinct
        to_timestamp(se.ts, 'YYYY-MM-DD HH:MI:SS') as timeId,
        se.userId,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionId,
        se.location,
        se.userAgent
    from
        staging_songs ss
    inner join
        staging_events se on se.songTitle = ss.title
        and se.artistName = ss.artist_name
    where se.page='NextSong'
""")

user_table_insert = ("""
    insert into users (
        userId,
        userFirstName,
        userLastName,
        gender,
        level
    )
    select distinct
        se.userId,
        se.userFirstName,
        se.userLastName,
        se.gender,
        se.level
    from
        staging_events se
    where
        se.userId is not null and se.page='NextSong'
""")

song_table_insert = ("""
    insert into songs (
        songId,
        songTitle,
        artistId,
        year,
        duration
    )
    select distinct
        ss.song_id,
        ss.title,
        ss.artist_id,
        ss.year,
        ss.duration
    from
        staging_songs ss
""")

artist_table_insert = ("""
    insert into artists (
        artistId,
        artistName,
        artistLocation,
        artistLatitude,
        artistLongitude
    )
    select distinct
        ss.artist_id,
        ss.artist_name,
        ss.artist_location,
        ss.artist_latitude,
        ss.artist_longitude
    from
        staging_songs ss
""")

time_table_insert = ("""
    insert into time (
        timeId,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    select distinct
        to_timestamp(se.ts, 'YYYY-MM-DD HH:MI:SS') as timeId,
        extract(hour from to_timestamp(se.ts, 'YYYY-MM-DD HH:MI:SS')) as hour,
        extract(day from to_timestamp(se.ts, 'YYYY-MM-DD HH:MI:SS')) as day,
        extract(week from to_timestamp(se.ts, 'YYYY-MM-DD HH:MI:SS')) as week,
        extract(month from to_timestamp(se.ts, 'YYYY-MM-DD HH:MI:SS')) as month,
        extract(year from to_timestamp(se.ts, 'YYYY-MM-DD HH:MI:SS')) as year,
        extract(dow from to_timestamp(se.ts, 'YYYY-MM-DD HH:MI:SS')) as weekday
    from
        staging_events se
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
