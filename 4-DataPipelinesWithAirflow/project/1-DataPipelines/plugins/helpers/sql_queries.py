class SqlQueries:
    
    songplay_table_insert = ("""
        INSERT  INTO public.songplays
        SELECT  distinct
                md5(se.sessionid || se.start_time) playid
                ,se.start_time
                ,se.userid
                ,se.level
                ,ss.song_id
                ,ss.artist_id
                ,se.sessionid
                ,se.location
                ,se.useragent
        FROM    (
                SELECT  TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
                FROM    staging_events
                WHERE   page = 'NextSong'
                ) se
                LEFT JOIN staging_songs ss
                    ON se.song = ss.title AND se.artist = ss.artist_name AND se.length = ss.duration
    """)

    user_table_insert = ("""
        INSERT  INTO public.users
        SELECT  distinct
                se.userid
                ,se.firstname
                ,se.lastname
                ,se.gender
                ,se.level
        FROM    staging_events se
        WHERE   page = 'NextSong'
    """)

    song_table_insert = ("""
        INSERT  INTO public.songs
        SELECT  distinct
                ss.song_id
                ,ss.title
                ,ss.artist_id
                ,ss.year
                ,ss.duration
        FROM    staging_songs ss
    """)

    artist_table_insert = ("""
        INSERT  INTO public.artists
        SELECT  distinct
                ss.artist_id
                ,ss.artist_name
                ,ss.artist_location
                ,ss.artist_latitude
                ,ss.artist_longitude
        FROM    staging_songs ss
    """)

    time_table_insert = ("""
        INSERT  INTO public.time
        SELECT  distinct
                sp.start_time
                ,extract(hour from sp.start_time)
                ,extract(day from sp.start_time)
                ,extract(week from sp.start_time)
                ,extract(month from sp.start_time)
                ,extract(year from sp.start_time)
                ,extract(dayofweek from sp.start_time)
        FROM    songplays sp
    """)