
{{
    config(
        alias='responsavel_unidade_armazenadora',
        description="Responsáveis pelas Unidades Armazenadoras"
    )
}}

SELECT
  SAFE_CAST(CD_UNIDADE_ARMAZENADORA AS INT64) AS id_unidade_armazenadora,
  SAFE_CAST(MATRICULA_RESPONSAVEL AS INT64) AS matricula_responsavel,
  SAFE_CAST(NM_RESPONSAVEL AS STRING) AS nome_responsavel,
  SAFE_CAST(DT_RESPONSAVEL AS STRING) AS data_inicio_responsavel,
  SAFE_CAST(MATRICULA_SUBSTITUTO1 AS INT64) AS matricula_substituto_1,
  SAFE_CAST(NM_SUBSTITUTO1 AS STRING) AS nome_substituto_1,
  SAFE_CAST(DT_SUBSTITUTO1 AS STRING) AS data_inicio_substituto_1,
  SAFE_CAST(MATRICULA_SUBSTITUTO2 AS INT64) AS matricula_substituto_2,
  SAFE_CAST(NM_SUBSTITUTO2 AS STRING) AS nome_substituto_2,
  SAFE_CAST(DT_SUBSTITUTO2 AS STRING) AS data_inicio_substituto_2
from {{ source('brutos_compras_materiais_servicos_sigma_staging', 'responsavel_unidade_armazenadora')}}