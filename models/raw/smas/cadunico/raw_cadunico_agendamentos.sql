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
        DISTINCT
        id,
        id_capacidade,
        nome_completo,
        REGEXP_REPLACE(cpf, r'[\.\-]', '') as cpf,
        telefone,
        tipo,
        data_hora,
        unidade_nome,
        unidade_endereco,
        unidade_bairro,
        current_timestamp() as processed_at,
        DATE(data_hora) as data_particao
    from {{ source('brutos_data_metrica', 'cadunico_agendamentos') }}
    where data_hora is not null
)

select * from source_data