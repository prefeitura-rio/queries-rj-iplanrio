{{
    config(
      alias="conselho",
      description="Conselhos profissionais de classe utilizados para registro e validação das qualificações de funcionários.",
    )
}}

select
    safe_cast(`CONS_SG_CONSELHO` as string) as conselho_sigla,
    safe_cast(`CONS_DS_CONSELHO` as string) as conselho_descricao,
    safe_cast(SUBSTR(_prefect_extracted_at,1,10) AS datetime) AS datalake_loaded_at,
    safe_cast(current_timestamp()as datetime) AS datalake_transformed_at
FROM {{ source('brutos_osinfo_rh_staging', 'dc_conselho') }} 

