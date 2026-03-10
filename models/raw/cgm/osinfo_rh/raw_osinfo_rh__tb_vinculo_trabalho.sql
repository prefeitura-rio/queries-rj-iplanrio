{{
    config(
      alias="vinculo_trabalho",
      description="Vínculos de trabalho de funcionários, detalhando seus contratos, como admissão, cargo e jornada.",
      materialized='table'
    )
}}

select
    safe_cast(`VINC_CD_VINCULO` as integer) as vinculo_codigo,
    safe_cast(`FUNC_CD_CPF` as int64) as funcionario_cpf,
    safe_cast(`VINC_IN_ATIVO` as int64) as vinculo_ativo,
    safe_cast(`COD_OS` as int64) as organizacao_social_codigo,
    safe_cast(`VINC_NR_CNPJ_OS` as string) as organizacao_social_cnpj,
    safe_cast(`CBO_CD_CBO` as string) as cbo_codigo,
    safe_cast(`VINC_NR_REGISTRO_PROFISSIONAL` as string) as registro_profissional_numero,
    safe_cast(`CONS_SG_CONSELHO` as string) as conselho_sigla,
    safe_cast(`UF_CD_IBGE_CONSELHO` as decimal) as conselho_uf_ibge,
    safe_cast(`IDCATEGORIA` as integer) as id_categoria,
    safe_cast(`VINC_SETOR` as string) as vinculo_setor,
    safe_cast(`CD_TIPO_VINCULACAO` as string) as tipo_vinculacao_codigo,
    safe_cast(`VINC_VL_CARGA_HORARIA` as float64) as carga_horaria,
    safe_cast(`VINC_DT_INICIO` as date) as vinculo_inicio_data,
    safe_cast(`VINC_DT_ENCERRAMENTO` as date) as vinculo_encerramento_data,
    safe_cast(`ENDE_CD_ENDERECO` as integer) as endereco_codigo,
    safe_cast(`VINC_DS_EMAIL` as string) as email,
    safe_cast(`VINC_CD_CNES` as decimal) as cnes_codigo,
    safe_cast(`VINC_DS_CARGO` as string) as cargo_descricao,
    safe_cast(`VINC_NR_TELEFONE` as string) as telefone_numero,
    safe_cast(SUBSTR(_prefect_extracted_at,1,10) AS datetime) AS datalake_loaded_at,
    safe_cast(current_timestamp()as datetime) AS datalake_transformed_at
FROM {{ source('brutos_osinfo_rh_staging', 'tb_vinculo_trabalho') }}
