{{
    config(
        alias="chatbot_base_disparo",
        schema="rmi_conversas",
        materialized="incremental",
        tags=["hourly"],
        unique_key=["id_hsm", "id_disparo", "contato_telefone", "criacao_envio_datahora"],
        partition_by={
            "field": "data_particao",
            "data_type": "date"
        }
    )
}}
select * from {{ ref('int_chatbot_base_disparo') }}
{% if is_incremental() %}
    where data_particao > (select max(data_particao) from {{ this }})
{% endif %}
