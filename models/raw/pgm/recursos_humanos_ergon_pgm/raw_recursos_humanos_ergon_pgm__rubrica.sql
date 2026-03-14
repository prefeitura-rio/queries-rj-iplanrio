{{
    config(
        alias='rubrica_ergon',
        materialized="table",
        tags=["raw", "ergon", "rubricas"],
        description="Tabela que armazena informações sobre rubricas de pagamento."
    )
}}

select id_rubrica, 
  tipo_rubrica, 
  nome, 
  nome_abreviado, 
  fator, 
  observacoes
FROM {{ ref('raw_recursos_humanos_ergon__rubrica') }} AS t
