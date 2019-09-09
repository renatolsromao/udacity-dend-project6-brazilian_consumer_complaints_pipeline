consumidorgovbr_queries = {

    'drop_stage_table': """
        drop table if exists staging.cgb;
    """,

    'create_stage_table': """
        create table staging.cgb (
            gestor text,
            canal text, 
            regiao varchar(2),
            UF char(2),
            cidade text,
            sexo char(1),
            faixa_etaria text,
            ano_abertura int,
            mes_abertura int, 
            data_abertura date,
            hora_abertura text,
            data_resposta date,
            hora_resposta text,
            data_analise date,
            hora_analise text,
            data_recusa date,
            hora_recusa text,
            data_finalizacao date,
            hora_finalizacao text,
            prazo_resposta date,
            prazo_analise_gestor int,
            tempo_resposta int, 
            nome_fantasia text,
            segmento_mercado text,
            area text,
            assunto text,
            grupo_problema text,
            problema text,
            como_comprou text,
            procurou_empresa char(1),
            respondida char(1),
            situação text,
            avaliacao text,
            nota float,
            analise_recusa text,
            edicao_conteudo char(1),
            interacao_gestor char(1),
            total int
        );
    """,

    'insert_dm_date': """
        insert into dm_date (ts, year, quarter, month, day, day_of_week) 
        select distinct
            data_abertura as ts,    
            date_part('y', data_abertura) as year,
            date_part('qtr', data_abertura) as quarter,
            date_part('mon', data_abertura) as month,
            date_part('d', data_abertura) as day,
            date_part('dow', data_abertura) as day_of_week
        from staging.cgb as c
        left join dm_date as d on c.data_abertura = d.ts
        where d.ts is null;
    """,

    'insert_dm_region': """
        insert into dm_region (city, state, region)
        select distinct
            cidade,
            uf,
            regiao
        from staging.cgb as c
        left join dm_region as r on c.cidade = r.city
        where r.city is null
    """,

    'insert_dm_consumer_profile': """
        insert into dm_consumer_profile (age, gender)
        select distinct
            faixa_etaria as age,
            sexo as gender
        from staging.cgb as cgb
        left join dm_consumer_profile as c on (c.age = cgb.faixa_etaria and c.gender = cgb.sexo)
        where c.age is null;
    """,

    'insert_dm_company': """
        insert into dm_company (name, segment)
        select distinct
            nome_fantasia,
            segmento_mercado
        from staging.cgb as cgb
        left join dm_company as c on c.name = cgb.nome_fantasia
        where c.name is null;
    """,

    'insert_ft_complaints': """
        insert into ft_complaints (ts, city, consumer_id, company_name, type, channel, time_to_answer, rating)
        select
            data_abertura as ts,
            cidade as city,
            c.consumer_id,
            nome_fantasia as company_name,
            area as type,
            canal as channel,
            tempo_resposta as time_to_answer,
            nota as rating
        from staging.cgb as cgb
        left join dm_consumer_profile as c on (cgb.faixa_etaria = c.age and cgb.sexo = c.gender)
    """,

}
