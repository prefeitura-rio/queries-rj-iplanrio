{{
    config(
        alias='matricula',
        materialized="table",
        tags=["raw", "ergon", "matricula"],
        description="Tabela que guarda as matrículas dadas pela prefeitura do Rio de Janeiro para pessoas que devem receber pagamento ou para pessoas em outros casos de interesse da prefeitura do Rio de Janeiro."
    )
}}

SELECT
    SAFE_CAST(NUMFUNC AS string) AS id_funcionario,
    SAFE_CAST(MATRIC AS string) AS id_matricula
FROM {{ source('brutos_recursos_humanos_ergon', 'vw_dlk_erg_erg_matriculas') }} AS t