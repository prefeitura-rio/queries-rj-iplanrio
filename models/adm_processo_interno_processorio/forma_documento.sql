SELECT
    SAFE_CAST(REGEXP_REPLACE(id_forma_doc, r'\.0$', '') AS	STRING) AS 	id_forma_documento,
    SAFE_CAST(descr_forma_doc AS string) AS descricao_forma_documento,
    SAFE_CAST(sigla_forma_doc AS string) AS sigla_forma_doccumento,
    SAFE_CAST(REGEXP_REPLACE(id_tipo_forma_doc, r'\.0$', '') AS	STRING) AS 	id_tipo_forma_documento,
FROM `rj-iplanrio.adm_processo_interno_processorio_staging.forma_documento`
