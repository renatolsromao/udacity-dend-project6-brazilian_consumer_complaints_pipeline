dimensions_queries = {

    'drop_dm_date_table': """
        drop table if exists dm_date;
    """,

    'create_dm_date_table': """
        create table if not exists dm_date (
            ts timestamptz PRIMARY KEY,
            year int,
            quarter int,
            month int,
            day int,
            day_of_week int,
            hour int,
            minute int  
        );
    """,

    'create_dm_region': """
        create table if not exists dm_region (
            city text PRIMARY KEY,
            region text,
            macroregion text
        );
    """,

    'create_dm_consumer': """
        create table if not exists dm_consumer (
            consumer_id int identity(0,1) primary key,
            age text,
            gender text
        );
    """,

    'create_dm_company': """
        create table if not exists dm_company (
            name text primary key,
            segment text
        );
    """,

}