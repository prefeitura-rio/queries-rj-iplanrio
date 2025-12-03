{{
    config( alias='funcionario_evento',schema='recursos_humanos_ergon_comlurb')
}}
SELECT
    SAFE_CAST(REGEXP_REPLACE(TRIM(numfunc), r'\.0$', '') AS STRING) AS id_funcionario,
    SAFE_CAST(REGEXP_REPLACE(TRIM(numev), r'\.0$', '') AS STRING) AS id_evento,
    SAFE_CAST(TRIM(referencia) AS STRING) AS referencia,
    SAFE_CAST(TRIM(jornada) AS STRING) AS horas_jornada,
    SAFE_CAST(REGEXP_REPLACE(TRIM(horariotrab), r'\.0$', '') AS STRING) AS id_horario_trabalho,
    SAFE_CAST(REGEXP_REPLACE(TRIM(emp_codigo), r'\.0$', '') AS STRING) AS id_empresa,
    SAFE_CAST(REGEXP_REPLACE(TRIM(id_reg), r'\.0$', '') AS STRING) AS id_registro,
    SAFE_CAST(REGEXP_REPLACE(TRIM(numvinc), r'\.0$', '') AS STRING) AS id_vinculo,
    SAFE_CAST(TRIM(tipoevento) AS STRING) AS tipo_evento,
    SAFE_CAST(TRIM(formaprov) AS STRING) AS forma_provimento,
    SAFE_CAST(DATE(dtini) AS DATE) AS data_inicio_situacao_funcional,
    SAFE_CAST(DATE(dtfim) AS DATE) AS data_fim_situacao_funcional,
    SAFE_CAST(DATE(dtinirem) AS DATE) AS data_inicio_evento_funcional,
    SAFE_CAST(DATE(dtfimrem) AS DATE) AS data_fim_evento_funcional,
    SAFE_CAST(REGEXP_REPLACE(TRIM(cargo), r'\.0$', '') AS STRING) AS id_cargo,
    SAFE_CAST(REGEXP_REPLACE(TRIM(setor), r'\.0$', '') AS STRING) AS id_setor,
FROM rj-smfp.recursos_humanos_ergon_comlurb_staging.funcionario_evento AS t