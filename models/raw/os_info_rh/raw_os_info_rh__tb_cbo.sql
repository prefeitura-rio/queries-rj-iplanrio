{{
    config(
      alias="tb_cbo",
      description="Ocupações da Classificação Brasileira de Ocupações (CBO) com seus códigos e descrições oficiais."
    )
}}

select
    safe_cast(`CBO_CD_CBO` as string) as cbo_codigo,
    safe_cast(`CBO_DS_CBO` as string) as cbo_descricao,
    safe_cast(`CBO_IN_ATIVIDADE_FIM` as string) as cbo_atividade_fim,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at
FROM {{ source('os_info_rh', 'tb_cbo') }}
