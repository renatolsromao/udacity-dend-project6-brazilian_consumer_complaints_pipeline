cep_queries = {

    'create_cep_table': """
        create table if not exists cep (
            cep int,
            logradouro text,
            bairro text,
            cidade_id int, 
            estado_id int
        );
    """,

    'create_cities_table': """
        create table if not exists cities (
            cidade_id int,
            cidade_nome text,
            estado_id int
        );    
    """,

    'create_states_table': """
        create table if not exists states (
            estado_id int,
            estado_nome text,
            estado_sigla char(2)
        );
    """

}
