fact_queries = {

    'drop_ft_complaints': """
        drop table if exists ft_complaints;
    """,

    'create_ft_complaints': """
        create table if not exists ft_complaints (
            complaint_id INT IDENTITY(0,1) primary key,
            ts timestamptz,
            city text,
            consumer_id text,
            company_name text,
            type text,
            channel text,
            time_to_answer int,
            rating int,
            foreign key(ts) references dm_date(ts),
            foreign key(city) references dm_region(city),
            foreign key(consumer_id) references dm_consumer_profile(consumer_id),
            foreign key(company_name) references dm_company(name)
        );
    """

}
