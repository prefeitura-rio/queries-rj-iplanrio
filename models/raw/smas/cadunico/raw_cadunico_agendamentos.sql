{{
    config(
        schema="brutos_data_metrica_staging",
        materialized="table",
        partition_by={
            "field": "data_hora",
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
        PARSE_DATETIME("%Y-%m-%d %H:%M:%S", data_hora) as data_hora,
        unidade_nome,
        unidade_endereco,
        unidade_bairro,
        current_timestamp() as processed_at
    from {{ source('brutos_data_metrica', 'agendamentos_cadunico') }}
    where data_hora is not null
)

select * from source_data