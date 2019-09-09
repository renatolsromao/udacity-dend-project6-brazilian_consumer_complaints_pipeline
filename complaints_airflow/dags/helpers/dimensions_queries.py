dimensions_queries = {

    'drop_dm_date_table': """
        drop table if exists dm_date;
    """,

    'create_dm_date': """
        create table if not exists dm_date (
            ts timestamptz PRIMARY KEY,
            year int,
            quarter int,
            month int,
            day int,
            day_of_week int
        );
    """,

    'drop_dm_region_table': """
        drop table if exists dm_region;
    """,

    'create_dm_region': """
        create table if not exists dm_region (
            city text PRIMARY KEY,
            state text,
            region text
        );
    """,

    'drop_dm_consumer_profile_table': """
        drop table if exists dm_consumer_profile;
    """,

    'create_dm_consumer_profile': """
        create table if not exists dm_consumer_profile (
            consumer_id int identity(0,1) primary key,
            age text,
            gender text
        );
    """,

    'drop_dm_company_table': """
        drop table if exists dm_company;
    """,

    'create_dm_company': """
        create table if not exists dm_company (
            name text primary key,
            segment text
        );
    """,

}