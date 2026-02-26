{{
    config(
      alias="funcionario",
      description="Funcionários com suas informações cadastrais, pessoais e profissionais, para gestão de recursos humanos.",
      materialized='table'
    )
}}

select
    safe_cast(FUNC_CD_CPF as numeric) as funcionario_cpf,
    safe_cast(FUNC_DS_NOME as string) as funcionario_nome,
    safe_cast(FUNC_DS_NOME_SOCIAL as string) as funcionario_nome_social,
    safe_cast(FUNC_DT_NASCIMENTO as date) as nascimento_data,
    safe_cast(FUNC_DS_NOME_MAE as string) as nome_mae,
    safe_cast(FUNC_DS_NOME_PAI as string) as nome_pai,
    safe_cast(FUNC_CD_NACIONALIDADE as int64) as nacionalidade_codigo,
    safe_cast(FUNC_DS_NACIONALIDADE as string) as nacionalidade_descricao,
    safe_cast(SEX_CD_SEXO as int64) as sexo_codigo,
    safe_cast(GINS_CD_GRAU_INSTRUCAO as int64) as grau_instrucao_codigo,
    safe_cast(MUNI_CD_IBGE_NASCIMENTO as numeric) as municipio_nascimento_ibge,
    safe_cast(RACO_CD_RACA_COR as int64) as raca_cor_codigo,
    safe_cast(FUNC_NR_RG as string) as rg_numero,
    safe_cast(FUNC_DS_ORGAO_EXPEDIDOR_RG as string) as rg_orgao_expedidor,
    safe_cast(FUNC_DT_EMISSAO as date) as rg_emissao_data,
    safe_cast(COD_OS as int64) as organizacao_social_codigo,
    safe_cast(m.uf_cd_ibge as string) as uf_codigo_ibge,
    SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S', f._prefect_extracted_at) as datalake_loaded_at,
    safe_cast(current_timestamp() as datetime) AS datalake_transformed_at
FROM {{ source('brutos_osinfo_rh_staging', 'tb_funcionario') }} f
LEFT JOIN {{ source('brutos_osinfo_rh_staging', 'dc_municipio') }} m 
    ON f.MUNI_CD_IBGE_NASCIMENTO = m.MUNI_CD_IBGE
