{{
    config(
        alias='tb_funcionario',
        schema='os_info_rh'
    )
}}

SELECT
    FUNC_CD_CPF,
    FUNC_DS_NOME,
    FUNC_DS_NOME_SOCIAL,
    FUNC_DT_NASCIMENTO,
    FUNC_DS_NOME_MAE,
    FUNC_DS_NOME_PAI,
    FUNC_CD_NACIONALIDADE,
    FUNC_DS_NACIONALIDADE,
    SEX_CD_SEXO,
    GINS_CD_GRAU_INSTRUCAO,
    MUNI_CD_IBGE_NASCIMENTO,
    RACO_CD_RACA_COR,
    FUNC_NR_RG,
    FUNC_DS_ORGAO_EXPEDIDOR_RG,
    FUNC_DT_EMISSAO,
    COD_OS
FROM {{ source('os_info_rh', 'tb_funcionario') }}
