{{
    config(
        alias='afastamento_antigo_nomes',
        materialized="table",
        tags=["raw", "ergon", "afastamento"],
        description="Tabela que armazena informações sobre os códigos de afastamentos utilizados em sistemas da Prefeitura do Rio de Janeiro."
    )
}}

SELECT
    t.id_empresa,
    t.id_afastamento,
    t.nome_afastamento
FROM {{ ref('raw_recursos_humanos_ergon__afastamento_antigo_nomes') }} AS t


