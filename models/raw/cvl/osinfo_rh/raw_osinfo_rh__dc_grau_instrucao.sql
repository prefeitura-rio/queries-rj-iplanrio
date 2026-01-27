{{
    config(
      alias="grau_instrucao",
      description="Graus de instrução que classificam os níveis de escolaridade de indivíduos para fins de cadastro."
    )
}}

select
    safe_cast(`GINS_CD_GRAU_INSTRUCAO` as integer) as grau_instrucao_codigo,
    safe_cast(`GINS_DS_GRAU_INSTRUCAO` as string) as grau_instrucao_descricao,
    safe_cast(`GINS_ORDEM` as integer) as grau_instrucao_ordem,
    safe_cast(SUBSTR(_prefect_extracted_at,1,10) AS DATE) AS datalake_transformed_at
FROM {{ source('brutos_osinfo_rh_staging', 'dc_grau_instrucao') }}
