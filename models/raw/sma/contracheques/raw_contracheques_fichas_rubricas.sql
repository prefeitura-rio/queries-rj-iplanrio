{{
    config(
      alias="fichas_rubricas",
      materialized="table",
      tags=["raw", "contracheques"],
      description="Tabela de FICHAS_RUBRICAS"
    )
}}

select
    safe_cast(RUBRICA as integer) as rubrica,
    safe_cast(FICHA as integer) as ficha,
    safe_cast(LINHA as integer) as linha,
    safe_cast(VALOR as decimal) as valor,
    safe_cast(DESC_VANT as string) as desc_vant,
    safe_cast(COMPLEMENTO as string) as complemento,
    safe_cast(MES_ANO_DIREITO as string) as mes_ano_direito,
    safe_cast(INFO as string) as info,
    safe_cast(_prefect_extracted_at as timestamp) as datalake_transformed_at
FROM {{ source('brutos_contracheques_staging', 'ERGON.FICHAS_RUBRICAS') }}
