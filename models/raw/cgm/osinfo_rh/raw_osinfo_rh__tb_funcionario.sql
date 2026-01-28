{{
    config(
      alias="funcionario",
      description="Funcionários com suas informações cadastrais, pessoais e profissionais, para gestão de recursos humanos.",
      materialized='table'
    )
}}

select
    safe_cast(FUNC_CD_CPF as decimal) as funcionario_cpf,
    safe_cast(FUNC_DS_NOME as string) as funcionario_nome,
    safe_cast(FUNC_DS_NOME_SOCIAL as string) as funcionario_nome_social,
    safe_cast(FUNC_DT_NASCIMENTO as date) as nascimento_data,
    safe_cast(FUNC_DS_NOME_MAE as string) as nome_mae,
    safe_cast(FUNC_DS_NOME_PAI as string) as nome_pai,
    safe_cast(FUNC_CD_NACIONALIDADE as integer) as nacionalidade_codigo,
    safe_cast(FUNC_DS_NACIONALIDADE as string) as nacionalidade_descricao,
    safe_cast(SEX_CD_SEXO as integer) as sexo_codigo,
    safe_cast(GINS_CD_GRAU_INSTRUCAO as integer) as grau_instrucao_codigo,
    safe_cast(MUNI_CD_IBGE_NASCIMENTO as decimal) as municipio_nascimento_ibge,
    safe_cast(RACO_CD_RACA_COR as integer) as raca_cor_codigo,
    safe_cast(FUNC_NR_RG as string) as rg_numero,
    safe_cast(FUNC_DS_ORGAO_EXPEDIDOR_RG as string) as rg_orgao_expedidor,
    safe_cast(FUNC_DT_EMISSAO as date) as rg_emissao_data,
    safe_cast(COD_OS as string) as organizacao_social_codigo,
    safe_cast(_prefect_extracted_at AS datetime) AS datalake_loaded_at,
    safe_cast(current_timestamp() AS datetime) AS datalake_transformed_at
FROM {{ source('brutos_osinfo_rh_staging', 'tb_funcionario') }}
