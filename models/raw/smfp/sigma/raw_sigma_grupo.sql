{{
    config(
        alias='grupo',
        schema='compras_materiais_servicos_sigma'
    )
}}

SELECT
    SAFE_CAST(cd_grupo AS STRING) AS codigo_grupo,
    SAFE_CAST(ds_grupo AS STRING) AS descricao_grupo,
    SAFE_CAST(st_status AS STRING) AS status
from {{ source('compras_materiais_servicos_sigma_staging', 'VW_GRUPO')}}