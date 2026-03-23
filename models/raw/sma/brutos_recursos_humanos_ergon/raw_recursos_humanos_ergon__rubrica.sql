{{
    config(
        alias='rubrica_ergon',
        materialized="table",
        tags=["raw", "ergon", "rubricas"],
        description="Tabela que armazena informações sobre rubricas de pagamento."
    )
}}

/*SELECT *
FROM {{ source('brutos_ergon_staging', 'RUBRICAS') }} AS t
*/

select safe_cast(rubrica as string) as id_rubrica, 
  safe_cast(tiporubr as string) as tipo_rubrica, 
  safe_cast(nome as string) as nome, 
  safe_cast(nome_abrev as string) as nome_abreviado, 
  safe_cast(fat_vant as int64) as fator, 
  safe_cast(flex_campo_16 as string) as observacoes
FROM {{ source('brutos_ergon_staging', 'RUBRICAS') }} AS t
