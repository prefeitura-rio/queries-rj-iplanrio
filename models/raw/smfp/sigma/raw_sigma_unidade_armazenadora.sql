{{
    config(
        alias='unidade_armazenadora',
        schema='compras_materiais_servicos_sigma'
    )
}}

SELECT
    SAFE_CAST(cd_programa_trabalho AS STRING) AS id_programa_trabalho,
    SAFE_CAST(cd_unidade_administrativa AS STRING) AS id_unidade_administrativa,
    SAFE_CAST(cd_unidade_armazenadora AS STRING) AS id_unidade_armazenadora,
    SAFE_CAST(cnes AS STRING) AS id_cnes,
    SAFE_CAST(ds_unidade_administrativa AS STRING) AS descricao_unidade_administrativa,
    SAFE_CAST(ds_unidade_armazenadora AS STRING) AS descricao_unidade_armazenadora,
    SAFE_CAST(st_expr_monetaria_oito_casas AS STRING) AS expressao_monetaria,
    SAFE_CAST(st_status AS STRING) AS status_unidade_administrativa,
    SAFE_CAST(tp_almoxarifado AS STRING) AS tipo_almoxarifado,
    SAFE_CAST(tp_unidade_armazenadora AS STRING) AS tipo_unidade_armazenadora,
from {{ source('compras_materiais_servicos_sigma_staging', 'VW_UNIDADE_ARMAZENADORA')}}