WITH d2012_a_2019 AS (
(SELECT
  SAFE_CAST(id_designacao as STRING) as id_unidade_escolar,
  SAFE_CAST(id_turma as STRING) as id_turma,
  SAFE_CAST(id_aluno as STRING) as id_aluno,
  SAFE_CAST(ano as INT64) as ano,
  SAFE_CAST(coc as INT64) as bimestre,
  SAFE_CAST(disciplina as STRING) as disciplina,
  SAFE_CAST(REPLACE(nota, ",", ".") as FLOAT64) as nota
FROM `rj-sme.educacao_basica_avaliacao_staging.bimestral_2012`

UNION ALL

SELECT
  SAFE_CAST(id_designacao as STRING) as id_unidade_escolar,
  SAFE_CAST(id_turma as STRING) as id_turma,
  SAFE_CAST(id_aluno as STRING) as id_aluno,
  SAFE_CAST(ano as INT64) as ano,
  SAFE_CAST(coc as INT64) as bimestre,
  SAFE_CAST(disciplina as STRING) as disciplina,
  SAFE_CAST(REPLACE(nota, ",", ".") as FLOAT64) as nota
FROM `rj-sme.educacao_basica_avaliacao_staging.bimestral_2013`

UNION ALL

SELECT
  SAFE_CAST(id_designacao as STRING) as id_unidade_escolar,
  SAFE_CAST(id_turma as STRING) as id_turma,
  SAFE_CAST(id_aluno as STRING) as id_aluno,
  SAFE_CAST(ano as INT64) as ano,
  SAFE_CAST(coc as INT64) as bimestre,
  SAFE_CAST(disciplina as STRING) as disciplina,
  SAFE_CAST(REPLACE(nota, ",", ".") as FLOAT64) as nota
FROM `rj-sme.educacao_basica_avaliacao_staging.bimestral_2014`

UNION ALL

SELECT
  SAFE_CAST(id_designacao as STRING) as id_unidade_escolar,
  SAFE_CAST(id_turma as STRING) as id_turma,
  SAFE_CAST(id_aluno as STRING) as id_aluno,
  SAFE_CAST(ano as INT64) as ano,
  SAFE_CAST(coc as INT64) as bimestre,
  SAFE_CAST(disciplina as STRING) as disciplina,
  SAFE_CAST(REPLACE(nota, ",", ".") as FLOAT64) as nota
FROM `rj-sme.educacao_basica_avaliacao_staging.bimestral_2015`

UNION ALL

SELECT
  SAFE_CAST(id_designacao as STRING) as id_unidade_escolar,
  SAFE_CAST(id_turma as STRING) as id_turma,
  SAFE_CAST(id_aluno as STRING) as id_aluno,
  SAFE_CAST(ano as INT64) as ano,
  SAFE_CAST(coc as INT64) as bimestre,
  SAFE_CAST(disciplina as STRING) as disciplina,
  SAFE_CAST(REPLACE(nota, ",", ".") as FLOAT64) as nota
FROM `rj-sme.educacao_basica_avaliacao_staging.bimestral_2016`

UNION ALL

SELECT
  SAFE_CAST(id_designacao as STRING) as id_unidade_escolar,
  SAFE_CAST(id_turma as STRING) as id_turma,
  SAFE_CAST(id_aluno as STRING) as id_aluno,
  SAFE_CAST(ano as INT64) as ano,
  SAFE_CAST(coc as INT64) as bimestre,
  SAFE_CAST(disciplina as STRING) as disciplina,
  SAFE_CAST(REPLACE(nota, ",", ".") as FLOAT64) as nota
FROM `rj-sme.educacao_basica_avaliacao_staging.bimestral_2017`

UNION ALL

SELECT
  SAFE_CAST(id_designacao as STRING) as id_unidade_escolar,
  SAFE_CAST(id_turma as STRING) as id_turma,
  SAFE_CAST(id_aluno as STRING) as id_aluno,
  SAFE_CAST(ano as INT64) as ano,
  SAFE_CAST(coc as INT64) as bimestre,
  SAFE_CAST(disciplina as STRING) as disciplina,
  SAFE_CAST(REPLACE(nota, ",", ".") as FLOAT64) as nota
FROM `rj-sme.educacao_basica_avaliacao_staging.bimestral_2018`

UNION ALL

SELECT
  SAFE_CAST(id_designacao as STRING) as id_unidade_escolar,
  SAFE_CAST(id_turma as STRING) as id_turma,
  SAFE_CAST(id_aluno as STRING) as id_aluno,
  SAFE_CAST(ano as INT64) as ano,
  SAFE_CAST(coc as INT64) as bimestre,
  SAFE_CAST(disciplina as STRING) as disciplina,
  SAFE_CAST(REPLACE(nota, ",", ".") as FLOAT64) as nota
FROM `rj-sme.educacao_basica_avaliacao_staging.bimestral_2019`)
)

SELECT
  TRIM(id_aluno) as id_aluno_original,
  SUBSTR(SHA256(
        CONCAT(
            '{{ var("HASH_SEED") }}',
            TRIM(id_aluno)
        )
    ), 2,17) as  id_aluno,
  * EXCEPT(id_aluno)
FROM d2012_a_2019
WHERE id_aluno IS NOT NULL OR nota IS NOT NULL
ORDER BY ano, bimestre, id_aluno, disciplina
