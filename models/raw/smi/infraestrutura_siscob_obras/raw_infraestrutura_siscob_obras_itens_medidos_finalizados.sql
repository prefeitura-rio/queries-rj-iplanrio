{{
    config(
        alias="itens_medidos_finalizados",
        schema='infraestrutura_siscob_obras',
        materialized="table",
    )
}}

SELECT
    DISTINCT
        *
from {{ source('brutos_siscob_staging', 'itens_medidos_finalizados') }}