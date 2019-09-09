cep_queries = {

    'drop_staging_cep': """
        drop table if exists staging.cep;
    """,

    'create_cep_table': """
        create table if not exists staging.cep (
            cep int,
            logradouro text,
            bairro text,
            cidade_id int, 
            estado_id int
        );
    """,

    'drop_staging_cities': """
        drop table if exists staging.cities;
    """,

    'create_cities_table': """
        create table if not exists staging.cities (
            cidade_id int,
            cidade_nome text,
            estado_id int
        );    
    """,

    'drop_staging_states': """
        drop table if exists staging.states;
    """,

    'create_states_table': """
        create table if not exists staging.states (
            estado_id int,
            estado_nome text,
            estado_sigla char(2)
        );
    """

}
