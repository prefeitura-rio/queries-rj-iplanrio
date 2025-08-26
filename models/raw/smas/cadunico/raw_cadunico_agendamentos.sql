{{
    config(
        schema="brutos_data_metrica_staging",
        materialized="table",
        partition_by={
            "field": "data_particao",
            "data_type": "date"
        }
    )
}}

with source_data as (
    select
        id,
        id_capacidade,
        nome_completo,
        cpf,
        telefone,
        tipo,
        data_hora,
        unidade_nome,
        unidade_endereco,
        unidade_bairro,
        current_timestamp() as processed_at,
        DATE(data_hora) as data_particao
    from {{ source('brutos_data_metrica', 'agendamentos_cadunico') }}
    where data_hora is not null and cpf is not null and id is not null
)

select * from source_data