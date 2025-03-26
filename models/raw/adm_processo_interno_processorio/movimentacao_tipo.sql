SELECT
   SAFE_CAST(REGEXP_REPLACE(id_tp_mov, r'\.0$', '') AS	STRING) AS 	id_tipo_movimentacao,
   SAFE_CAST(descr_tipo_movimentacao AS STRING) AS descricao_tipo_movimentacao,
FROM `rj-iplanrio.adm_processo_interno_processorio_staging.movimentacao_tipo`