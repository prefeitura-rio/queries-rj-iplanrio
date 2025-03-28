select
    SAFE_CAST(SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", dt_primeiraassinatura) AS DATETIME) AS data_primeira_assinatura ,
    SAFE_CAST(SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E*S",dt_finalizacao) AS DATETIME) AS data_finalizacao,
    SAFE_CAST(REGEXP_REPLACE(ultimo_id_mov, r'\.0$', '') AS	STRING) AS 	id_ultimo_movimento,
    SAFE_CAST(sigla_doc AS  STRING) AS sigla_documento,
    SAFE_CAST(arquivado AS	STRING) AS arquivado,
    SAFE_CAST(REGEXP_REPLACE(id_mobil, r'\.0$', '') AS	STRING) AS 	id_modbil,
    CAST(SPLIT(tempo_tramitacao, ' days')[OFFSET(0)] AS INT64) AS dias_tramitacao,
    SAFE_CAST(tempo_tramitacao AS	STRING) AS tempo_tramitacao,
    SAFE_CAST(REGEXP_REPLACE(id_lota_resp, r'\.0$', '') AS	STRING) AS 	id_lotacao_responsavel,
    SAFE_CAST(lotacao_resp	AS	STRING )AS	nome_lotacao_responsavel,
    SAFE_CAST(SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S",data_com_resp_atual)	AS	datetime) AS data_responsavel_atual,
FROM `rj-iplanrio.adm_processo_interno_processorio_staging.documento_tempo`
