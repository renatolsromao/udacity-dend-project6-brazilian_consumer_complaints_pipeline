dm_region_table = 'dm_region'
dm_date_table = 'dm_date'
dm_consumer_profile_table = 'dm_consumer_profile'
dm_company_table = 'dm_company'

dimensions_queries = {

    'drop_dm_date_table': f"""
        drop table if exists {dm_date_table};
    """,

    'create_dm_date': f"""
        create table if not exists {dm_date_table} (
            ts timestamptz PRIMARY KEY,
            year int,
            quarter int,
            month int,
            day int,
            day_of_week int
        );
    """,

    'drop_dm_region_table': f"""
        drop table if exists {dm_region_table};
    """,

    'create_dm_region': f"""
        create table if not exists {dm_region_table} (
            city text PRIMARY KEY,
            state text,
            region text
        );
    """,

    'drop_dm_consumer_profile_table': f"""
        drop table if exists {dm_consumer_profile_table};
    """,

    'create_dm_consumer_profile': f"""
        create table if not exists {dm_consumer_profile_table} (
            consumer_id int identity(0,1) primary key,
            age text,
            gender text
        );
    """,

    'drop_dm_company_table': f"""
        drop table if exists {dm_company_table};
    """,

    'create_dm_company': f"""
        create table if not exists {dm_company_table} (
            name text primary key,
            segment text
        );
    """,

}