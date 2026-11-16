{{
    config(
        alias='vw_pessoa',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(ic_sexo AS STRING) AS ic_sexo,
    SAFE_CAST(id_bairro_logradouro_fk AS INT64) AS id_bairro_logradouro_fk,
    SAFE_CAST(id_cargo_fk AS INT64) AS id_cargo_fk,
    SAFE_CAST(id_escolaridade_fk AS INT64) AS id_escolaridade_fk,
    SAFE_CAST(id_idioma_fk AS INT64) AS id_idioma_fk,
    SAFE_CAST(id_pais_ddi_fk AS INT64) AS id_pais_ddi_fk,
    SAFE_CAST(id_pessoa AS INT64) AS id_pessoa,
    SAFE_CAST(id_tipo_endereco_fk AS INT64) AS id_tipo_endereco_fk,
    SAFE_CAST(id_tratamento_fk AS INT64) AS id_tratamento_fk,
    SAFE_CAST(id_unidade_organizacional_fk AS INT64) AS id_unidade_organizacional_fk,
    SAFE_CAST(no_mae AS STRING) AS no_mae,
    SAFE_CAST(no_pessoa AS STRING) AS no_pessoa,
    SAFE_CAST(nu_coord_x AS INT64) AS nu_coord_x,
    SAFE_CAST(nu_coord_y AS INT64) AS nu_coord_y,
    SAFE_CAST(nu_matricula AS STRING) AS nu_matricula,
    SAFE_CAST(nu_prazo_expira_senha AS INT64) AS nu_prazo_expira_senha,
FROM {{ source('brutos_1746_staging_airbyte', 'vw_pessoa') }}
