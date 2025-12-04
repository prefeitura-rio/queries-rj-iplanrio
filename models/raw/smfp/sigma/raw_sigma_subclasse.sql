{{
    config(
        alias='subclasse',
        description="Subclasse das Classes da Lista de Classificação"
    )
}}

SELECT
    SAFE_CAST(cd_classe AS STRING) AS codigo_classe,
    SAFE_CAST(cd_grupo AS STRING) AS codigo_grupo,
    SAFE_CAST(cd_subclasse AS STRING) AS codigo_subclasse,
    SAFE_CAST(ds_subclasse AS STRING) AS descricao_subclasse,
    SAFE_CAST(st_status AS STRING) AS id_status,
from {{ source('brutos_compras_materiais_servicos_sigma_staging', 'subclasse')}}