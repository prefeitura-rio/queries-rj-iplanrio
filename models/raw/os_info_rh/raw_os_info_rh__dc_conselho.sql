{{
    config(
      alias="dc_conselho",
      description="Conselhos profissionais de classe utilizados para registro e validação das qualificações de funcionários."
    )
}}

select
    safe_cast(`CONS_SG_CONSELHO` as string) as conselho_sigla,
    safe_cast(`CONS_DS_CONSELHO` as string) as conselho_descricao,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at
FROM {{ source('os_info_rh', 'dc_conselho') }}
