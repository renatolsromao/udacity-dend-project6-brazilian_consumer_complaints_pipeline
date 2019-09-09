brzipcode_queries = {

    'drop_staging_brzipcode': """
        drop table if exists staging.brzipcode;
    """,

    'create_brzipcode_table': """
        create table if not exists staging.brzipcode (
            zipcode int primary key,
            address text,
            address_type text,
            neighborhood text,
            city_id int,
            city text,
            state text,
            state_abbr char(2),
            nf_city_id int
        );
    """,

}
