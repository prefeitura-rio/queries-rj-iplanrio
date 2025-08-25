
{{
    config(
        alias='exclusao_servidor',
        schema='protecao_social_cadunico',
        materialized="table",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        }
    )
}}


SELECT

    --column: chv_natural_prefeitura_fam_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_exclusao,

    --column: cod_familiar_fam_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_exclusao,

    --column: cpf_oper_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,11))
        END AS STRING
    ) AS cpf_operador_exclusao,

    --column: cpf_servd_pbco_pgmcu_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,149,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,149,11))
        END AS STRING
    ) AS cpf_servidor_parecer_gestao_municipal_cadunico,

    --column: dat_emi_pgmcu
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,8))
        END    ) AS data_emissao_parecer_gestao_municipal_cadunico,

    --column: data_exc
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_exclusao,

    --column: desc_mot_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,169,255), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,169,255))
        END AS STRING
    ) AS descricao_cotivo_exclusao,

    --column: motivo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_motivo_exclusao,
    --column: motivo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^01$') THEN 'Falecimento De Toda A Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^02$') THEN 'Recusa Da Família Em Prestar Informações'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^03$') THEN 'Omissão Ou Prestação De Informações Inverídicas Pela Família, Por Comprovada Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^04$') THEN 'Solicitação Do RUF'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^05$') THEN 'Decisão Judicial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^06$') THEN 'Cadastros Desatualizados Cuja Inclusão Ou Última Atualização Ocorreu Há 48 (Quarenta E Oito) Meses Ou Mais'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^07$') THEN 'Decurso De Prazo No Estado Cadastral “Em Cadastramento”'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^08$') THEN 'Exclusão/Inativação Ocorrida Antes Da Versão 7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^09$') THEN 'Família Transferida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^10$') THEN 'Família Com Renda Per Capita Acima De ½ Salário Mínimo E Renda Familiar Acima De 3 Salários Mínimos E Que Não Esteja Vinculada A Nenhum Programa Social'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^11$') THEN 'Cadastro Desatualizado Há Mais De 48 Meses'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^12$') THEN 'Averiguação Cadastral'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^13$') THEN 'Exclusão De Todos Os Integrantes Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^14$') THEN 'Exclusão/Transferência De Todos Os Integrantes Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^15$') THEN 'Averiguação - Suspeita De Fraude'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^16$') THEN 'Averiguação - Suspeita De Fraude Identificada Pelo Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^17$') THEN 'Família Com Renda Per Capita Acima De Meio Salário Mínimo E Que Não Esteja Vinculada A Nenhum Programa Social'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^18$') THEN 'Identificação De Cadastros Incluídos Ou Alterados Indevidamente Por Agente Público Por Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^19$') THEN 'Solicitação Do RUF Via APP'
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS motivo_exclusao,

    --column: mun_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,162,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,162,7))
        END AS STRING
    ) AS id_municipio_parecer_gestao_municipal_cadunico,

    --column: nom_servd_pbco_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,70))
        END AS STRING
    ) AS servidor_parecer_gestao_municipal_cadunico,

    --column: num_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,10))
        END AS STRING
    ) AS numero_parecer_gestao_municipal_cadunico,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: uf_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,160,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,160,2))
        END AS STRING
    ) AS sigla_uf_parecer_gestao_municipal_cadunico,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0601'
    AND SUBSTRING(text,38,2) = '18'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_exclusao,

    --column: cod_familiar_fam_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_exclusao,

    --column: cpf_oper_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,11))
        END AS STRING
    ) AS cpf_operador_exclusao,

    --column: cpf_servd_pbco_pgmcu_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,149,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,149,11))
        END AS STRING
    ) AS cpf_servidor_parecer_gestao_municipal_cadunico,

    --column: dat_emi_pgmcu
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,8))
        END    ) AS data_emissao_parecer_gestao_municipal_cadunico,

    --column: data_exc
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_exclusao,

    --column: desc_mot_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,169,255), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,169,255))
        END AS STRING
    ) AS descricao_cotivo_exclusao,

    --column: motivo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_motivo_exclusao,
    --column: motivo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^01$') THEN 'Falecimento De Toda A Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^02$') THEN 'Recusa Da Família Em Prestar Informações'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^03$') THEN 'Omissão Ou Prestação De Informações Inverídicas Pela Família, Por Comprovada Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^04$') THEN 'Solicitação Do RUF'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^05$') THEN 'Decisão Judicial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^06$') THEN 'Cadastros Desatualizados Cuja Inclusão Ou Última Atualização Ocorreu Há 48 (Quarenta E Oito) Meses Ou Mais'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^07$') THEN 'Decurso De Prazo No Estado Cadastral “Em Cadastramento”'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^08$') THEN 'Exclusão/Inativação Ocorrida Antes Da Versão 7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^09$') THEN 'Família Transferida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^10$') THEN 'Família Com Renda Per Capita Acima De ½ Salário Mínimo E Renda Familiar Acima De 3 Salários Mínimos E Que Não Esteja Vinculada A Nenhum Programa Social'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^11$') THEN 'Cadastro Desatualizado Há Mais De 48 Meses'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^12$') THEN 'Averiguação Cadastral'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^13$') THEN 'Exclusão De Todos Os Integrantes Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^14$') THEN 'Exclusão/Transferência De Todos Os Integrantes Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^15$') THEN 'Averiguação - Suspeita De Fraude'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^16$') THEN 'Averiguação - Suspeita De Fraude Identificada Pelo Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^17$') THEN 'Família Com Renda Per Capita Acima De Meio Salário Mínimo E Que Não Esteja Vinculada A Nenhum Programa Social'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^18$') THEN 'Identificação De Cadastros Incluídos Ou Alterados Indevidamente Por Agente Público Por Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^19$') THEN 'Solicitação Do RUF Via APP'
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS motivo_exclusao,

    --column: mun_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,162,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,162,7))
        END AS STRING
    ) AS id_municipio_parecer_gestao_municipal_cadunico,

    --column: nom_servd_pbco_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,70))
        END AS STRING
    ) AS servidor_parecer_gestao_municipal_cadunico,

    --column: num_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,10))
        END AS STRING
    ) AS numero_parecer_gestao_municipal_cadunico,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: uf_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,160,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,160,2))
        END AS STRING
    ) AS sigla_uf_parecer_gestao_municipal_cadunico,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0603'
    AND SUBSTRING(text,38,2) = '18'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_exclusao,

    --column: cod_familiar_fam_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_exclusao,

    --column: cpf_oper_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,11))
        END AS STRING
    ) AS cpf_operador_exclusao,

    --column: cpf_servd_pbco_pgmcu_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,149,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,149,11))
        END AS STRING
    ) AS cpf_servidor_parecer_gestao_municipal_cadunico,

    --column: dat_emi_pgmcu
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,8))
        END    ) AS data_emissao_parecer_gestao_municipal_cadunico,

    --column: data_exc
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_exclusao,

    --column: desc_mot_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,169,255), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,169,255))
        END AS STRING
    ) AS descricao_cotivo_exclusao,

    --column: motivo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_motivo_exclusao,
    --column: motivo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^01$') THEN 'Falecimento De Toda A Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^02$') THEN 'Recusa Da Família Em Prestar Informações'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^03$') THEN 'Omissão Ou Prestação De Informações Inverídicas Pela Família, Por Comprovada Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^04$') THEN 'Solicitação Do RUF'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^05$') THEN 'Decisão Judicial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^06$') THEN 'Cadastros Desatualizados Cuja Inclusão Ou Última Atualização Ocorreu Há 48 (Quarenta E Oito) Meses Ou Mais'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^07$') THEN 'Decurso De Prazo No Estado Cadastral “Em Cadastramento”'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^08$') THEN 'Exclusão/Inativação Ocorrida Antes Da Versão 7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^09$') THEN 'Família Transferida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^10$') THEN 'Família Com Renda Per Capita Acima De ½ Salário Mínimo E Renda Familiar Acima De 3 Salários Mínimos E Que Não Esteja Vinculada A Nenhum Programa Social'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^11$') THEN 'Cadastro Desatualizado Há Mais De 48 Meses'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^12$') THEN 'Averiguação Cadastral'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^13$') THEN 'Exclusão De Todos Os Integrantes Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^14$') THEN 'Exclusão/Transferência De Todos Os Integrantes Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^15$') THEN 'Averiguação - Suspeita De Fraude'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^16$') THEN 'Averiguação - Suspeita De Fraude Identificada Pelo Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^17$') THEN 'Família Com Renda Per Capita Acima De Meio Salário Mínimo E Que Não Esteja Vinculada A Nenhum Programa Social'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^18$') THEN 'Identificação De Cadastros Incluídos Ou Alterados Indevidamente Por Agente Público Por Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^19$') THEN 'Solicitação Do RUF Via APP'
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS motivo_exclusao,

    --column: mun_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,162,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,162,7))
        END AS STRING
    ) AS id_municipio_parecer_gestao_municipal_cadunico,

    --column: nom_servd_pbco_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,70))
        END AS STRING
    ) AS servidor_parecer_gestao_municipal_cadunico,

    --column: num_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,10))
        END AS STRING
    ) AS numero_parecer_gestao_municipal_cadunico,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: uf_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,160,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,160,2))
        END AS STRING
    ) AS sigla_uf_parecer_gestao_municipal_cadunico,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0604'
    AND SUBSTRING(text,38,2) = '18'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_exclusao,

    --column: cod_familiar_fam_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_exclusao,

    --column: cpf_oper_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,11))
        END AS STRING
    ) AS cpf_operador_exclusao,

    --column: cpf_servd_pbco_pgmcu_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,149,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,149,11))
        END AS STRING
    ) AS cpf_servidor_parecer_gestao_municipal_cadunico,

    --column: dat_emi_pgmcu
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,8))
        END    ) AS data_emissao_parecer_gestao_municipal_cadunico,

    --column: data_exc
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_exclusao,

    --column: desc_mot_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,169,255), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,169,255))
        END AS STRING
    ) AS descricao_cotivo_exclusao,

    --column: motivo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_motivo_exclusao,
    --column: motivo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^01$') THEN 'Falecimento De Toda A Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^02$') THEN 'Recusa Da Família Em Prestar Informações'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^03$') THEN 'Omissão Ou Prestação De Informações Inverídicas Pela Família, Por Comprovada Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^04$') THEN 'Solicitação Do RUF'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^05$') THEN 'Decisão Judicial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^06$') THEN 'Cadastros Desatualizados Cuja Inclusão Ou Última Atualização Ocorreu Há 48 (Quarenta E Oito) Meses Ou Mais'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^07$') THEN 'Decurso De Prazo No Estado Cadastral “Em Cadastramento”'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^08$') THEN 'Exclusão/Inativação Ocorrida Antes Da Versão 7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^09$') THEN 'Família Transferida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^10$') THEN 'Família Com Renda Per Capita Acima De ½ Salário Mínimo E Renda Familiar Acima De 3 Salários Mínimos E Que Não Esteja Vinculada A Nenhum Programa Social'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^11$') THEN 'Cadastro Desatualizado Há Mais De 48 Meses'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^12$') THEN 'Averiguação Cadastral'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^13$') THEN 'Exclusão De Todos Os Integrantes Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^14$') THEN 'Exclusão/Transferência De Todos Os Integrantes Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^15$') THEN 'Averiguação - Suspeita De Fraude'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^16$') THEN 'Averiguação - Suspeita De Fraude Identificada Pelo Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^17$') THEN 'Família Com Renda Per Capita Acima De Meio Salário Mínimo E Que Não Esteja Vinculada A Nenhum Programa Social'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^18$') THEN 'Identificação De Cadastros Incluídos Ou Alterados Indevidamente Por Agente Público Por Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^19$') THEN 'Solicitação Do RUF Via APP'
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS motivo_exclusao,

    --column: mun_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,162,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,162,7))
        END AS STRING
    ) AS id_municipio_parecer_gestao_municipal_cadunico,

    --column: nom_servd_pbco_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,70))
        END AS STRING
    ) AS servidor_parecer_gestao_municipal_cadunico,

    --column: num_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,10))
        END AS STRING
    ) AS numero_parecer_gestao_municipal_cadunico,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: uf_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,160,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,160,2))
        END AS STRING
    ) AS sigla_uf_parecer_gestao_municipal_cadunico,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0609'
    AND SUBSTRING(text,38,2) = '18'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_exclusao,

    --column: cod_familiar_fam_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_exclusao,

    --column: cpf_oper_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,11))
        END AS STRING
    ) AS cpf_operador_exclusao,

    --column: cpf_servd_pbco_pgmcu_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,149,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,149,11))
        END AS STRING
    ) AS cpf_servidor_parecer_gestao_municipal_cadunico,

    --column: dat_emi_pgmcu
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,8))
        END    ) AS data_emissao_parecer_gestao_municipal_cadunico,

    --column: data_exc
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_exclusao,

    --column: desc_mot_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,169,255), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,169,255))
        END AS STRING
    ) AS descricao_cotivo_exclusao,

    --column: motivo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_motivo_exclusao,
    --column: motivo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^01$') THEN 'Falecimento De Toda A Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^02$') THEN 'Recusa Da Família Em Prestar Informações'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^03$') THEN 'Omissão Ou Prestação De Informações Inverídicas Pela Família, Por Comprovada Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^04$') THEN 'Solicitação Do RUF'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^05$') THEN 'Decisão Judicial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^06$') THEN 'Cadastros Desatualizados Cuja Inclusão Ou Última Atualização Ocorreu Há 48 (Quarenta E Oito) Meses Ou Mais'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^07$') THEN 'Decurso De Prazo No Estado Cadastral “Em Cadastramento”'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^08$') THEN 'Exclusão/Inativação Ocorrida Antes Da Versão 7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^09$') THEN 'Família Transferida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^10$') THEN 'Família Com Renda Per Capita Acima De ½ Salário Mínimo E Renda Familiar Acima De 3 Salários Mínimos E Que Não Esteja Vinculada A Nenhum Programa Social'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^11$') THEN 'Cadastro Desatualizado Há Mais De 48 Meses'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^12$') THEN 'Averiguação Cadastral'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^13$') THEN 'Exclusão De Todos Os Integrantes Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^14$') THEN 'Exclusão/Transferência De Todos Os Integrantes Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^15$') THEN 'Averiguação - Suspeita De Fraude'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^16$') THEN 'Averiguação - Suspeita De Fraude Identificada Pelo Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^17$') THEN 'Família Com Renda Per Capita Acima De Meio Salário Mínimo E Que Não Esteja Vinculada A Nenhum Programa Social'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^18$') THEN 'Identificação De Cadastros Incluídos Ou Alterados Indevidamente Por Agente Público Por Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^19$') THEN 'Solicitação Do RUF Via APP'
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS motivo_exclusao,

    --column: mun_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,162,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,162,7))
        END AS STRING
    ) AS id_municipio_parecer_gestao_municipal_cadunico,

    --column: nom_servd_pbco_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,70))
        END AS STRING
    ) AS servidor_parecer_gestao_municipal_cadunico,

    --column: num_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,10))
        END AS STRING
    ) AS numero_parecer_gestao_municipal_cadunico,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: uf_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,160,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,160,2))
        END AS STRING
    ) AS sigla_uf_parecer_gestao_municipal_cadunico,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0612'
    AND SUBSTRING(text,38,2) = '18'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_exclusao,

    --column: cod_familiar_fam_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_exclusao,

    --column: cpf_oper_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,11))
        END AS STRING
    ) AS cpf_operador_exclusao,

    --column: cpf_servd_pbco_pgmcu_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,149,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,149,11))
        END AS STRING
    ) AS cpf_servidor_parecer_gestao_municipal_cadunico,

    --column: dat_emi_pgmcu
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,8))
        END    ) AS data_emissao_parecer_gestao_municipal_cadunico,

    --column: data_exc
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_exclusao,

    --column: desc_mot_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,169,255), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,169,255))
        END AS STRING
    ) AS descricao_cotivo_exclusao,

    --column: motivo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_motivo_exclusao,
    --column: motivo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^01$') THEN 'Falecimento De Toda A Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^02$') THEN 'Recusa Da Família Em Prestar Informações'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^03$') THEN 'Omissão Ou Prestação De Informações Inverídicas Pela Família, Por Comprovada Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^04$') THEN 'Solicitação Do RUF'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^05$') THEN 'Decisão Judicial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^06$') THEN 'Cadastros Desatualizados Cuja Inclusão Ou Última Atualização Ocorreu Há 48 (Quarenta E Oito) Meses Ou Mais'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^07$') THEN 'Decurso De Prazo No Estado Cadastral “Em Cadastramento”'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^08$') THEN 'Exclusão/Inativação Ocorrida Antes Da Versão 7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^09$') THEN 'Família Transferida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^10$') THEN 'Família Com Renda Per Capita Acima De ½ Salário Mínimo E Renda Familiar Acima De 3 Salários Mínimos E Que Não Esteja Vinculada A Nenhum Programa Social'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^11$') THEN 'Cadastro Desatualizado Há Mais De 48 Meses'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^12$') THEN 'Averiguação Cadastral'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^13$') THEN 'Exclusão De Todos Os Integrantes Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^14$') THEN 'Exclusão/Transferência De Todos Os Integrantes Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^15$') THEN 'Averiguação - Suspeita De Fraude'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^16$') THEN 'Averiguação - Suspeita De Fraude Identificada Pelo Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^17$') THEN 'Família Com Renda Per Capita Acima De Meio Salário Mínimo E Que Não Esteja Vinculada A Nenhum Programa Social'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^18$') THEN 'Identificação De Cadastros Incluídos Ou Alterados Indevidamente Por Agente Público Por Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^19$') THEN 'Solicitação Do RUF Via APP'
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS motivo_exclusao,

    --column: mun_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,162,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,162,7))
        END AS STRING
    ) AS id_municipio_parecer_gestao_municipal_cadunico,

    --column: nom_servd_pbco_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,70))
        END AS STRING
    ) AS servidor_parecer_gestao_municipal_cadunico,

    --column: num_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,10))
        END AS STRING
    ) AS numero_parecer_gestao_municipal_cadunico,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: uf_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,160,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,160,2))
        END AS STRING
    ) AS sigla_uf_parecer_gestao_municipal_cadunico,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0615'
    AND SUBSTRING(text,38,2) = '18'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_exclusao,

    --column: cod_familiar_fam_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_exclusao,

    --column: cpf_oper_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,11))
        END AS STRING
    ) AS cpf_operador_exclusao,

    --column: cpf_servd_pbco_pgmcu_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,149,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,149,11))
        END AS STRING
    ) AS cpf_servidor_parecer_gestao_municipal_cadunico,

    --column: dat_emi_pgmcu
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,8))
        END    ) AS data_emissao_parecer_gestao_municipal_cadunico,

    --column: data_exc
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_exclusao,

    --column: desc_mot_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,169,255), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,169,255))
        END AS STRING
    ) AS descricao_cotivo_exclusao,

    --column: motivo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_motivo_exclusao,
    --column: motivo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^01$') THEN 'Falecimento De Toda A Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^02$') THEN 'Recusa Da Família Em Prestar Informações'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^03$') THEN 'Omissão Ou Prestação De Informações Inverídicas Pela Família, Por Comprovada Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^04$') THEN 'Solicitação Do RUF'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^05$') THEN 'Decisão Judicial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^06$') THEN 'Cadastros Desatualizados Cuja Inclusão Ou Última Atualização Ocorreu Há 48 (Quarenta E Oito) Meses Ou Mais'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^07$') THEN 'Decurso De Prazo No Estado Cadastral “Em Cadastramento”'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^08$') THEN 'Exclusão/Inativação Ocorrida Antes Da Versão 7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^09$') THEN 'Família Transferida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^10$') THEN 'Família Com Renda Per Capita Acima De ½ Salário Mínimo E Renda Familiar Acima De 3 Salários Mínimos E Que Não Esteja Vinculada A Nenhum Programa Social'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^11$') THEN 'Cadastro Desatualizado Há Mais De 48 Meses'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^12$') THEN 'Averiguação Cadastral'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^13$') THEN 'Exclusão De Todos Os Integrantes Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^14$') THEN 'Exclusão/Transferência De Todos Os Integrantes Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^15$') THEN 'Averiguação - Suspeita De Fraude'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^16$') THEN 'Averiguação - Suspeita De Fraude Identificada Pelo Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^17$') THEN 'Família Com Renda Per Capita Acima De Meio Salário Mínimo E Que Não Esteja Vinculada A Nenhum Programa Social'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^18$') THEN 'Identificação De Cadastros Incluídos Ou Alterados Indevidamente Por Agente Público Por Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^19$') THEN 'Solicitação Do RUF Via APP'
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS motivo_exclusao,

    --column: mun_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,162,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,162,7))
        END AS STRING
    ) AS id_municipio_parecer_gestao_municipal_cadunico,

    --column: nom_servd_pbco_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,70))
        END AS STRING
    ) AS servidor_parecer_gestao_municipal_cadunico,

    --column: num_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,10))
        END AS STRING
    ) AS numero_parecer_gestao_municipal_cadunico,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: uf_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,160,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,160,2))
        END AS STRING
    ) AS sigla_uf_parecer_gestao_municipal_cadunico,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0617'
    AND SUBSTRING(text,38,2) = '18'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_exclusao,

    --column: cod_familiar_fam_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_exclusao,

    --column: cpf_oper_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,11))
        END AS STRING
    ) AS cpf_operador_exclusao,

    --column: cpf_servd_pbco_pgmcu_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,149,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,149,11))
        END AS STRING
    ) AS cpf_servidor_parecer_gestao_municipal_cadunico,

    --column: dat_emi_pgmcu
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,8))
        END    ) AS data_emissao_parecer_gestao_municipal_cadunico,

    --column: data_exc
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_exclusao,

    --column: desc_mot_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,169,255), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,169,255))
        END AS STRING
    ) AS descricao_cotivo_exclusao,

    --column: motivo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_motivo_exclusao,
    --column: motivo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^01$') THEN 'Falecimento De Toda A Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^02$') THEN 'Recusa Da Família Em Prestar Informações'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^03$') THEN 'Omissão Ou Prestação De Informações Inverídicas Pela Família, Por Comprovada Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^04$') THEN 'Solicitação Do RUF'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^05$') THEN 'Decisão Judicial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^06$') THEN 'Cadastros Desatualizados Cuja Inclusão Ou Última Atualização Ocorreu Há 48 (Quarenta E Oito) Meses Ou Mais'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^07$') THEN 'Decurso De Prazo No Estado Cadastral “Em Cadastramento”'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^08$') THEN 'Exclusão/Inativação Ocorrida Antes Da Versão 7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^09$') THEN 'Família Transferida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^10$') THEN 'Família Com Renda Per Capita Acima De ½ Salário Mínimo E Renda Familiar Acima De 3 Salários Mínimos E Que Não Esteja Vinculada A Nenhum Programa Social'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^11$') THEN 'Cadastro Desatualizado Há Mais De 48 Meses'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^12$') THEN 'Averiguação Cadastral'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^13$') THEN 'Exclusão De Todos Os Integrantes Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^14$') THEN 'Exclusão/Transferência De Todos Os Integrantes Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^15$') THEN 'Averiguação - Suspeita De Fraude'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^16$') THEN 'Averiguação - Suspeita De Fraude Identificada Pelo Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^17$') THEN 'Família Com Renda Per Capita Acima De Meio Salário Mínimo E Que Não Esteja Vinculada A Nenhum Programa Social'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^18$') THEN 'Identificação De Cadastros Incluídos Ou Alterados Indevidamente Por Agente Público Por Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^19$') THEN 'Solicitação Do RUF Via APP'
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS motivo_exclusao,

    --column: mun_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,162,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,162,7))
        END AS STRING
    ) AS id_municipio_parecer_gestao_municipal_cadunico,

    --column: nom_servd_pbco_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,70))
        END AS STRING
    ) AS servidor_parecer_gestao_municipal_cadunico,

    --column: num_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,10))
        END AS STRING
    ) AS numero_parecer_gestao_municipal_cadunico,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: uf_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,160,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,160,2))
        END AS STRING
    ) AS sigla_uf_parecer_gestao_municipal_cadunico,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0619'
    AND SUBSTRING(text,38,2) = '18'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_exclusao,

    --column: cod_familiar_fam_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_exclusao,

    --column: cpf_oper_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,11))
        END AS STRING
    ) AS cpf_operador_exclusao,

    --column: cpf_servd_pbco_pgmcu_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,149,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,149,11))
        END AS STRING
    ) AS cpf_servidor_parecer_gestao_municipal_cadunico,

    --column: dat_emi_pgmcu
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,8))
        END    ) AS data_emissao_parecer_gestao_municipal_cadunico,

    --column: data_exc
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_exclusao,

    --column: desc_mot_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,169,255), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,169,255))
        END AS STRING
    ) AS descricao_cotivo_exclusao,

    --column: motivo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_motivo_exclusao,
    --column: motivo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^01$') THEN 'Falecimento De Toda A Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^02$') THEN 'Recusa Da Família Em Prestar Informações'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^03$') THEN 'Omissão Ou Prestação De Informações Inverídicas Pela Família, Por Comprovada Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^04$') THEN 'Solicitação Do RUF'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^05$') THEN 'Decisão Judicial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^06$') THEN 'Cadastros Desatualizados Cuja Inclusão Ou Última Atualização Ocorreu Há 48 (Quarenta E Oito) Meses Ou Mais'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^07$') THEN 'Decurso De Prazo No Estado Cadastral “Em Cadastramento”'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^08$') THEN 'Exclusão/Inativação Ocorrida Antes Da Versão 7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^09$') THEN 'Família Transferida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^10$') THEN 'Família Com Renda Per Capita Acima De ½ Salário Mínimo E Renda Familiar Acima De 3 Salários Mínimos E Que Não Esteja Vinculada A Nenhum Programa Social'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^11$') THEN 'Cadastro Desatualizado Há Mais De 48 Meses'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^12$') THEN 'Averiguação Cadastral'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^13$') THEN 'Exclusão De Todos Os Integrantes Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^14$') THEN 'Exclusão/Transferência De Todos Os Integrantes Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^15$') THEN 'Averiguação - Suspeita De Fraude'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^16$') THEN 'Averiguação - Suspeita De Fraude Identificada Pelo Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^17$') THEN 'Família Com Renda Per Capita Acima De Meio Salário Mínimo E Que Não Esteja Vinculada A Nenhum Programa Social'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^18$') THEN 'Identificação De Cadastros Incluídos Ou Alterados Indevidamente Por Agente Público Por Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^19$') THEN 'Solicitação Do RUF Via APP'
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS motivo_exclusao,

    --column: mun_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,162,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,162,7))
        END AS STRING
    ) AS id_municipio_parecer_gestao_municipal_cadunico,

    --column: nom_servd_pbco_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,70))
        END AS STRING
    ) AS servidor_parecer_gestao_municipal_cadunico,

    --column: num_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,10))
        END AS STRING
    ) AS numero_parecer_gestao_municipal_cadunico,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: uf_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,160,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,160,2))
        END AS STRING
    ) AS sigla_uf_parecer_gestao_municipal_cadunico,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0620'
    AND SUBSTRING(text,38,2) = '18'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_exclusao,

    --column: cod_familiar_fam_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_exclusao,

    --column: cpf_oper_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,11))
        END AS STRING
    ) AS cpf_operador_exclusao,

    --column: cpf_servd_pbco_pgmcu_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,149,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,149,11))
        END AS STRING
    ) AS cpf_servidor_parecer_gestao_municipal_cadunico,

    --column: dat_emi_pgmcu
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,8))
        END    ) AS data_emissao_parecer_gestao_municipal_cadunico,

    --column: data_exc
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_exclusao,

    --column: desc_mot_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,169,255), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,169,255))
        END AS STRING
    ) AS descricao_cotivo_exclusao,

    --column: motivo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_motivo_exclusao,
    --column: motivo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^01$') THEN 'Falecimento De Toda A Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^02$') THEN 'Recusa Da Família Em Prestar Informações'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^03$') THEN 'Omissão Ou Prestação De Informações Inverídicas Pela Família, Por Comprovada Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^04$') THEN 'Solicitação Do RUF'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^05$') THEN 'Decisão Judicial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^06$') THEN 'Cadastros Desatualizados Cuja Inclusão Ou Última Atualização Ocorreu Há 48 (Quarenta E Oito) Meses Ou Mais'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^07$') THEN 'Decurso De Prazo No Estado Cadastral “Em Cadastramento”'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^08$') THEN 'Exclusão/Inativação Ocorrida Antes Da Versão 7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^09$') THEN 'Família Transferida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^10$') THEN 'Família Com Renda Per Capita Acima De ½ Salário Mínimo E Renda Familiar Acima De 3 Salários Mínimos E Que Não Esteja Vinculada A Nenhum Programa Social'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^11$') THEN 'Cadastro Desatualizado Há Mais De 48 Meses'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^12$') THEN 'Averiguação Cadastral'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^13$') THEN 'Exclusão De Todos Os Integrantes Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^14$') THEN 'Exclusão/Transferência De Todos Os Integrantes Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^15$') THEN 'Averiguação - Suspeita De Fraude'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^16$') THEN 'Averiguação - Suspeita De Fraude Identificada Pelo Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^17$') THEN 'Família Com Renda Per Capita Acima De Meio Salário Mínimo E Que Não Esteja Vinculada A Nenhum Programa Social'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^18$') THEN 'Identificação De Cadastros Incluídos Ou Alterados Indevidamente Por Agente Público Por Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^19$') THEN 'Solicitação Do RUF Via APP'
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS motivo_exclusao,

    --column: mun_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,162,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,162,7))
        END AS STRING
    ) AS id_municipio_parecer_gestao_municipal_cadunico,

    --column: nom_servd_pbco_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,70))
        END AS STRING
    ) AS servidor_parecer_gestao_municipal_cadunico,

    --column: num_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,10))
        END AS STRING
    ) AS numero_parecer_gestao_municipal_cadunico,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: uf_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,160,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,160,2))
        END AS STRING
    ) AS sigla_uf_parecer_gestao_municipal_cadunico,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0623'
    AND SUBSTRING(text,38,2) = '18'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_fam_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_exclusao,

    --column: cod_familiar_fam_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_exclusao,

    --column: cpf_oper_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,11))
        END AS STRING
    ) AS cpf_operador_exclusao,

    --column: cpf_servd_pbco_pgmcu_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,149,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,149,11))
        END AS STRING
    ) AS cpf_servidor_parecer_gestao_municipal_cadunico,

    --column: dat_emi_pgmcu
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,8))
        END    ) AS data_emissao_parecer_gestao_municipal_cadunico,

    --column: data_exc
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_exclusao,

    --column: desc_mot_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,169,255), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,169,255))
        END AS STRING
    ) AS descricao_cotivo_exclusao,

    --column: motivo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_motivo_exclusao,
    --column: motivo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^01$') THEN 'Falecimento De Toda A Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^02$') THEN 'Recusa Da Família Em Prestar Informações'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^03$') THEN 'Omissão Ou Prestação De Informações Inverídicas Pela Família, Por Comprovada Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^04$') THEN 'Solicitação Do RUF'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^05$') THEN 'Decisão Judicial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^06$') THEN 'Cadastros Desatualizados Cuja Inclusão Ou Última Atualização Ocorreu Há 48 (Quarenta E Oito) Meses Ou Mais'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^07$') THEN 'Decurso De Prazo No Estado Cadastral “Em Cadastramento”'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^08$') THEN 'Exclusão/Inativação Ocorrida Antes Da Versão 7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^09$') THEN 'Família Transferida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^10$') THEN 'Família Com Renda Per Capita Acima De ½ Salário Mínimo E Renda Familiar Acima De 3 Salários Mínimos E Que Não Esteja Vinculada A Nenhum Programa Social'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^11$') THEN 'Cadastro Desatualizado Há Mais De 48 Meses'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^12$') THEN 'Averiguação Cadastral'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^13$') THEN 'Exclusão De Todos Os Integrantes Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^14$') THEN 'Exclusão/Transferência De Todos Os Integrantes Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^15$') THEN 'Averiguação - Suspeita De Fraude'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^16$') THEN 'Averiguação - Suspeita De Fraude Identificada Pelo Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^17$') THEN 'Família Com Renda Per Capita Acima De Meio Salário Mínimo E Que Não Esteja Vinculada A Nenhum Programa Social'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^18$') THEN 'Identificação De Cadastros Incluídos Ou Alterados Indevidamente Por Agente Público Por Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^19$') THEN 'Solicitação Do RUF Via APP'
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS motivo_exclusao,

    --column: mun_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,162,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,162,7))
        END AS STRING
    ) AS id_municipio_parecer_gestao_municipal_cadunico,

    --column: nom_servd_pbco_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,70))
        END AS STRING
    ) AS servidor_parecer_gestao_municipal_cadunico,

    --column: num_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,10))
        END AS STRING
    ) AS numero_parecer_gestao_municipal_cadunico,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: uf_pgmcu
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,160,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,160,2))
        END AS STRING
    ) AS sigla_uf_parecer_gestao_municipal_cadunico,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-smas.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0624'
    AND SUBSTRING(text,38,2) = '18'

