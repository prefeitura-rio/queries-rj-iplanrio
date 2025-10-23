{{ config(
    alias='disciplinas_sem_professor'
) }}

SELECT
  SAFE_CAST(cre_nome AS STRING) as nome_cre,
  LPAD(SAFE_CAST(esc_codigo AS STRING), 7, "0") as codigo_escola,
  SAFE_CAST(esc_nome AS STRING) as nome_escola,
  SAFE_CAST(codigo_escola AS STRING) as codigo_escola_completo,
  SAFE_CAST(nivel_ensino AS STRING) as nivel_ensino,
  SAFE_CAST(etapa_ensino AS STRING) as etapa_ensino,
  SAFE_CAST(grupamento AS STRING) as grupamento,
  SAFE_CAST(tipo_turma AS STRING) as tipo_turma,
  SAFE_CAST(codigo_turma AS STRING) as codigo_turma,
  SAFE_CAST(disciplina AS STRING) as disciplina,
  SAFE_CAST(tempos_carencia as INT64) as tempos_carencia
FROM {{ source('sme_brutos_educacao_basica_alocacao_staging', 'disciplinas_sem_professor') }}
