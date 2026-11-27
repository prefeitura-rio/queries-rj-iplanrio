{{
    config(
        alias='provimento',
        materialized="table",
        tags=["raw", "ergon", "provimento"],
        description="Eventos relacionados aos vínculos funcionais tanto com a administração direta como indireta da prefeitura do Rio de Janeiro."
    )
}}

SELECT
    SAFE_CAST (NUMFUNC AS STRING) AS id_funcionario,
    SAFE_CAST (NUMVINC AS STRING) AS id_vinculo,
    DATETIME(DTINI) AS data_inicio,
    DATETIME(DTFIM) AS data_fim,
    SAFE_CAST (SETOR AS string) AS id_setor,
    SAFE_CAST (CARGO AS STRING) AS id_cargo,
    SAFE_CAST (REFERENCIA AS string) AS id_referencia,
    SAFE_CAST (JORNADA AS string) AS id_jornada,
    SAFE_CAST (FORMAPROV AS string) AS id_forma_provimento,
    SAFE_CAST (OBS AS STRING) AS observacoes,
    SAFE_CAST (FLEX_CAMPO_03 AS STRING) AS regime_horas,
    SAFE_CAST (EMP_CODIGO AS string) AS id_empresa,
    _prefect_extracted_at AS updated_at
FROM {{ source('brutos_recursos_humanos_ergon', 'vw_dlk_erg_provimentos_ev')}}
