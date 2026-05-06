CREATE OR REPLACE TABLE `rj-crm-registry-dev.case_ds.aluno` AS

WITH raw_aluno AS (
    SELECT * FROM `rj-sme.educacao_basica.aluno`
    where ano = 2025
)

SELECT
    id_aluno,
    FARM_FINGERPRINT(CONCAT(CAST(id_turma AS STRING), '53950274')) AS id_turma,

    -- Anonimizar idade em faixas
    CASE 
        WHEN idade_atual < 6 THEN '0-5'
        WHEN idade_atual BETWEEN 6 AND 10 THEN '6-10'
        WHEN idade_atual BETWEEN 11 AND 14 THEN '11-14'
        WHEN idade_atual BETWEEN 15 AND 17 THEN '15-17'
        ELSE '18+'
    END AS faixa_etaria,
    
    FARM_FINGERPRINT(CAST(bairro AS STRING)) as bairro,

FROM raw_aluno
;

---------------------------------------------------

CREATE OR REPLACE TABLE `rj-crm-registry-dev.case_ds.avaliacao` AS

WITH raw_avaliacao AS (
    -- Substitua pelo caminho real da sua tabela bruta
    SELECT * FROM `rj-sme.educacao_basica.avaliacao`
    WHERE ano = 2025
)

SELECT
    id_aluno,
    FARM_FINGERPRINT(CONCAT(CAST(id_turma AS STRING), '53950274')) AS id_turma,
    frequencia,
    id_coc as bimestre,
    -- Randomização mantendo distribuição (jitter +/- 0.2)
    LEAST(10.0, GREATEST(0.0, 
        cast(portugues as float64) + (MOD(ABS(FARM_FINGERPRINT(id_aluno)), 41) - 20) / 100.0
    )) AS disciplina_1,
    LEAST(10.0, GREATEST(0.0, 
        cast(ciencias as float64) + (MOD(ABS(FARM_FINGERPRINT(id_aluno)), 41) - 20) / 100.0
    )) AS disciplina_2,
    LEAST(10.0, GREATEST(0.0, 
        cast(ingles as float64) + (MOD(ABS(FARM_FINGERPRINT(id_aluno)), 41) - 20) / 100.0
    )) AS disciplina_3,
    LEAST(10.0, GREATEST(0.0, 
        cast(matematica as float64) + (MOD(ABS(FARM_FINGERPRINT(id_aluno)), 41) - 20) / 100.0
    )) AS disciplina_4,
    
FROM raw_avaliacao
;

--  when disciplina = 'Língua Portuguesa' then 'disciplina_1'
--     when disciplina = 'Ciências' then 'disciplina_2'
--     when disciplina = 'Língua Estrangeira' then 'disciplina_3'
--     when disciplina = 'Matemática' then 'disciplina_4'
--------------------------------------------------------------------------------

-- CREATE OR REPLACE TABLE `rj-crm-registry-dev.case_ds.escola` AS

-- SELECT
--     id_escola,
--     FARM_FINGERPRINT(CAST(nome AS STRING)) as nome,
--     FARM_FINGERPRINT(CAST(endereco AS STRING)) as endereco,
--     FARM_FINGERPRINT(CAST(bairro AS STRING)) as bairro,
--     FARM_FINGERPRINT(CAST(cep AS STRING)) as cep,
--     FARM_FINGERPRINT(CAST(email AS STRING)) as email,
--     FARM_FINGERPRINT(CAST(telefone AS STRING)) as telefone,
--     FARM_FINGERPRINT(CAST(direcao AS STRING)) as direcao,
--     id_cre,

-- FROM `rj-sme.educacao_basica.escola`

CREATE OR REPLACE TABLE `rj-crm-registry-dev.case_ds.escola` AS
 
WITH raw_escola AS (
    SELECT * FROM `rj-sme.educacao_basica.escola`
)
 
SELECT
    FARM_FINGERPRINT(CONCAT(CAST(e.id_escola AS STRING), '53950274')) AS id_escola,
    FARM_FINGERPRINT(CONCAT(CAST(bairro AS STRING), '53950274')) as bairro
FROM raw_escola e
;


------------------------------------------------------------------------------


CREATE OR REPLACE TABLE `rj-crm-registry-dev.case_ds.frequencia` AS

WITH raw_frequencia AS (
    -- Substitua pelo caminho real da sua tabela bruta
    SELECT * FROM `rj-sme.educacao_basica.frequencia`
)

SELECT
   FARM_FINGERPRINT(CONCAT(CAST(id_escola AS STRING), '53950274')) AS id_escola,
   id_aluno,
   FARM_FINGERPRINT(CONCAT(CAST(id_turma AS STRING), '53950274')) AS id_turma,
   REGEXP_REPLACE(cast(data_inicio as string), '^2025', '2000') as data_inicio,
   REGEXP_REPLACE(cast(data_fim as string), '^2025', '2000') as data_fim,
   case
    when disciplina = 'Língua Portuguesa' then 'disciplina_1'
    when disciplina = 'Ciências' then 'disciplina_2'
    when disciplina = 'Língua Estrangeira' then 'disciplina_3'
    when disciplina = 'Matemática' then 'disciplina_4'
    end as disciplina,
   100 - faltas_disciplina/dias_letivos as frequencia,
FROM raw_frequencia
where id_aluno in (SELECT distinct id_aluno FROM `rj-sme.educacao_basica.aluno` where ano = 2025)
--data_particao between date('2025-01-01') and date('2025-12-31')
and disciplina in ('Matemática', 'Ciências', 'Língua Portuguesa', 'Língua Estrangeira')
order by frequencia desc
;

---------------------------------------------------------------------------------


CREATE OR REPLACE TABLE `rj-crm-registry-dev.case_ds.turma` AS

SELECT
    2000 as ano,
    FARM_FINGERPRINT(CONCAT(CAST(id_turma AS STRING), '53950274')) AS id_turma,
    id_aluno,
    -- serie,
    -- turno,
    -- ano_letivo
FROM `rj-sme.educacao_basica.aluno_turma`
where ano = 2025
