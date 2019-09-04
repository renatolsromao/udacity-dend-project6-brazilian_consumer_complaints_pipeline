procon_queries = {

    'drop_stage_table': """
        drop table if exists {};
    """,

    'create_stage_table': """
        create table {} (
            ano_atendimento int,
            trimestre_atendimento int,
            mes_atendimento int,
            data_atendimento timestamp,
            codigo_regiao int,
            regiao text,
            uf char(2),
            codigo_tipo_atendimento int,
            descricao_tipo_atendimento text,
            cosigo_assunto int,
            descricao_assunto text,
            grupo_assunto text,
            codigo_problema int,
            descricao_problema text,
            grupo_problema text,
            sexo_consumidor char(1),
            faixa_etaria_consumidor text,
            cep_consumidor int,
            tipo_fornecedor int,
            razao_social_sindec text,
            nome_fantasia_sindec text,
            cnpj text,
            radical_cnpj text,
            razao_social_rfb text,
            nome_fantasia_rfv text,
            codigo_cnae_principal text,
            descricao_cnae_principal text
        );
    """,

    'insert_dm_date': """
        insert into dm_date (ts, year, quarter, month, day, day_of_week, hour, minute)
        select distinct 	
                data_atendimento as ts,    
                date_part('y', data_atendimento) as year,
                date_part('qtr', data_atendimento) as quarter,
                date_part('mon', data_atendimento) as month,
                date_part('d', data_atendimento) as day,
                date_part('dow', data_atendimento) as day_of_week,
                date_part('h', data_atendimento)::int as hour,
                date_part('m', data_atendimento)::int as minute
        from dadosabertosatendimentofornecedor1trimestre2017 as p
        left join dm_date as d on d.ts = p.data_atendimento
        where d.ts is null;
    """,

    'insert_dm_region': """
        insert into dm_region (city, region, macroregion)
        select distinct
            cit.cidade_nome,
            uf,
            regiao
        from dadosabertosatendimentofornecedor1trimestre2017 as proc
        left join cep as cep on cep.cep = proc.cep_consumidor
        left join cities as cit on cit.cidade_id = cep.cidade_id
        left join dm_region as reg on cit.cidade_nome = reg.city
        where 
            reg.city is null
            AND cit.cidade_nome is not null;
    """,

    'insert_dm_consumer': """
        select distinct
            faixa_etaria_consumidor as age,
            sexo_consumidor as gender
        from dadosabertosatendimentofornecedor1trimestre2017 as p
        left join dm_consumer as c on (c.age = p.faixa_etaria_consumidor and c.gender = p.sexo_consumidor)
        where c.age is null;
    """,

    'insert_dm_company': """
        insert into dm_company (name, segment)
        select distinct
            NVL(nome_fantasia_sindec, razao_social_sindec) as name,
            descricao_cnae_principal as segment
        from dadosabertosatendimentofornecedor1trimestre2017 as p
        left join dm_company as c on c.name = p.nome_fantasia_sindec
        where 
            c.name is null
            AND COALESCE(nome_fantasia_sindec, razao_social_sindec) is not null;
    """,

    'insert_ft_complaints': """
        insert into ft_complaints (ts, city, consumer_id, company_name, type, channel, time_to_answer, rating)
        select
            data_atendimento as ts,
            cit.cidade_nome as city,
            con.consumer_id,
            COALESCE(nome_fantasia_sindec, razao_social_sindec) as company_name,
            grupo_assunto as type,
            'procon' as channel,
            NULL as time_to_answer,
            NULL as rating
        from dadosabertosatendimentofornecedor1trimestre2017 as proc
        left join cep as cep on cep.cep = proc.cep_consumidor
        left join cities as cit on cit.cidade_id = cep.cidade_id
        left join dm_consumer as con on (proc.faixa_etaria_consumidor = con.age and proc.sexo_consumidor = con.gender)
        where
            COALESCE(nome_fantasia_sindec, razao_social_sindec) is not null;
    """,

}
