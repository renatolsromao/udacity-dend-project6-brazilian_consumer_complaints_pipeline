brzipcode_queries = {

    'drop_staging_brzipcode': """
        drop table if exists staging.brzipcode;
    """,

    'create_brzipcode_table': """
        create table if not exists staging.brzipcode (
            zipcode int primary key,
            city text,
            state text,
            state_abbr char(2),
            region varchar(2)
        );
    """,

}
