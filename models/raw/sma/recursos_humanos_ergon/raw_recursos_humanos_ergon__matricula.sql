{{
    config(
        alias='matricula',
        materialized="table",
        tags=["raw", "ergon", "matricula"],
        description="Tabela que guarda as matrículas dadas pela prefeitura do Rio de Janeiro para pessoas que devem receber pagamento ou para pessoas em outros casos de interesse da prefeitura do Rio de Janeiro."
    )
}}

SELECT
    SAFE_CAST(NUMFUNC AS int64) AS id_funcionario,
    SAFE_CAST(MATRIC AS string) AS id_matricula
FROM {{ source('brutos_ergon_staging', 'VW_DLK_ERG_ERG_MATRICULAS') }} AS t