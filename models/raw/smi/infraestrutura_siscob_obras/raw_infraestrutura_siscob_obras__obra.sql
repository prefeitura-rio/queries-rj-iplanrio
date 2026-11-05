{{
    config(
        alias="obra",
        schema='infraestrutura_siscob_obras',
        materialized="table",
    )
}}

SELECT 
    DISTINCT
        SAFE_CAST(cd_obra AS STRING) id_obra,
        SAFE_CAST(
            nr_processo AS STRING
        ) id_processo,
        SAFE_CAST(
            nr_licitacao AS STRING
        ) id_licitacao,
        SAFE_CAST(
            nr_contrato AS STRING
        ) id_contrato,
        SAFE_CAST(ds_titulo AS STRING) titulo,
        SAFE_CAST(orgao_contratante AS STRING) orgao_contratante,
        SAFE_CAST(orgao_executor AS STRING) orgao_executor,
        SAFE_CAST(objeto AS STRING) objeto,
        SAFE_CAST(nm_favorecido AS STRING) favorecido,
        SAFE_CAST(cnpj AS STRING) cnpj,
        SAFE_CAST(modalidade AS STRING) modalidade,
        SAFE_CAST(situacao AS STRING) situacao,
        SAFE_CAST(aa_exercicio AS INT64) ano_inicio_contrato,
        SAFE_CAST(
         dt_ass_contrato AS DATETIME
        ) AS data_assinatura_contrato,
        SAFE_CAST(dt_inicio_obra AS DATE) AS data_inicio,
        SAFE_CAST(dt_termino_previsto AS DATE) AS data_termino_previsto, 
        SAFE_CAST(dt_termino_atual AS DATE) AS data_termino_atual,
        SAFE_CAST(vl_orcado_c_bdi AS FLOAT64) valor_orcado,
        SAFE_CAST(vl_contratado AS FLOAT64) valor_contratado,
        SAFE_CAST(vl_vigente AS FLOAT64) valor_vigente,
        SAFE_CAST(pc_medido AS FLOAT64) percentual_medido,
        SAFE_CAST(prazo_inicial AS INT64) prazo_inicial
FROM {{ source('brutos_siscob_staging', 'obra') }} AS t