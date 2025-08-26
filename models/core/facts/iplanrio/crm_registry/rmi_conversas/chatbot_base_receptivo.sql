{{
    config(
        alias="chatbot_base_receptivo",
        schema="rmi_conversas",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["id_sessao", "inicio_datahora"],
        partition_by={
            "field": "data_particao",
            "data_type": "date"
        }
    )
}}


select * from {{ ref('int_chatbot_base_receptivo') }}
{% if is_incremental() %}
    where data_particao > (select max(data_particao) from {{ this }})
{% endif %}
