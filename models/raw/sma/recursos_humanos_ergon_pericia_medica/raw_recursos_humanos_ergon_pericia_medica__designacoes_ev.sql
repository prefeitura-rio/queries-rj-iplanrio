{{
    config(
        alias='designacoes_ev'
       
    )
}}

SELECT safe_cast(OBS AS STRING) AS obs, 
safe_cast(DTFIM AS DATE) AS dt_fim, 
safe_cast(DTINI AS DATE) AS dt_inicio, 
safe_cast(SETOR AS STRING) AS setor, 
safe_cast(FUNCAO AS STRING) AS funcao, 
safe_cast(ID_REG AS STRING) AS id_reg, 
safe_cast(NUMFUNC AS STRING) AS num_func, 
safe_cast(NUMVINC AS STRING) AS num_vinc, 
safe_cast(PONTLEI AS STRING) AS ponto_lei, 
safe_cast(PONTPUBL AS STRING) AS ponto_publicado, 
safe_cast(EMP_CODIGO AS STRING) AS emp_codigo, 
safe_cast(NUMERO_VAGA AS STRING) AS numero_vaga, 
safe_cast(NUMFUNCSUBS1 AS STRING) AS num_func_subs1, 
safe_cast(NUMFUNCSUBS2 AS STRING) AS num_func_subs2, 
safe_cast(NUMVINCSUBS1 AS STRING) AS num_vinc_subs1, 
safe_cast(NUMVINCSUBS2 AS STRING) AS num_vinc_subs2, 
safe_cast(FLEX_CAMPO_01 AS STRING) AS flex_campo_01, 
safe_cast(FLEX_CAMPO_02 AS STRING) AS flex_campo_02, 
safe_cast(FLEX_CAMPO_03 AS STRING) AS flex_campo_03, 
safe_cast(FLEX_CAMPO_04 AS STRING) AS flex_campo_04, 
safe_cast(FLEX_CAMPO_05 AS STRING) AS flex_campo_05 
FROM{{ source('brutos_recursos_humanos_ergon_pericia_medica_staging', 'DESIGNACOES_EV') }}

