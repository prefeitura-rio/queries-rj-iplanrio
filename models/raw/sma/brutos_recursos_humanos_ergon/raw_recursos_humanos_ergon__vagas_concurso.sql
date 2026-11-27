{{
    config(
        alias='vagas_concurso',
        materialized="table",
        tags=["raw", "ergon", "concursos", "vagas"],
        description="Vagas de concursos públicos."
    )
}}

SELECT
    SAFE_CAST(chave AS STRING) AS id_vaga,
    SAFE_CAST(chave_concurso AS STRING) AS id_concurso,
    SAFE_CAST(cargo AS STRING) AS id_cargo,
    SAFE_CAST(especialidade AS STRING) AS especialidade,
    SAFE_CAST(hlocal AS STRING) AS hlocal,
    SAFE_CAST(qtd_vagas AS INT64) AS quantidade_vagas,
    SAFE_CAST(qtd_vagas_def AS INT64) AS quantidade_vagas_deficientes,
    SAFE_CAST(flex_campo_01 AS STRING) AS flex_campo_01,
    SAFE_CAST(flex_campo_02 AS STRING) AS flex_campo_02,
    SAFE_CAST(flex_campo_03 AS STRING) AS flex_campo_03,
    SAFE_CAST(flex_campo_04 AS STRING) AS flex_campo_04,
    SAFE_CAST(flex_campo_05 AS STRING) AS flex_campo_05,
    SAFE_CAST(emp_codigo AS STRING) AS id_empresa,
    SAFE_CAST(sexo AS STRING) AS sexo,
    SAFE_CAST(percentual_vagas_def AS INT64) AS percentual_vagas_deficientes,
    SAFE_CAST(dias_prazo_posse AS INT64) AS dias_prazo_posse,
    SAFE_CAST(dias_prazo_prorrog_posse AS INT64) AS dias_prazo_prorrog_posse,
    SAFE_CAST(dias_prazo_exerc AS INT64) AS dias_prazo_exerc,
    SAFE_CAST(dias_prazo_prorrog_exerc AS INT64) AS dias_prazo_prorrog_exerc,
    SAFE_CAST(grupo_vagas AS STRING) AS grupo_vagas,
    SAFE_CAST(flex_campo_06 AS STRING) AS flex_campo_06,
    SAFE_CAST(flex_campo_07 AS STRING) AS flex_campo_07,
    SAFE_CAST(flex_campo_08 AS STRING) AS flex_campo_08,
    SAFE_CAST(flex_campo_09 AS STRING) AS flex_campo_09,
    SAFE_CAST(flex_campo_10 AS STRING) AS flex_campo_10,
    _prefect_extracted_at AS updated_at
FROM {{ source('brutos_recursos_humanos_ergon', 'vagas_concurso') }} AS t
