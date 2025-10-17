
{{
    config(
        alias='exclusao_membro',
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

    --column: chv_natural_prefeitura_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_membro_excluido,

    --column: cod_familiar_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_membro_excluido,

    --column: cod_folha_termo_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,247,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,247,4))
        END AS STRING
    ) AS folha_ceritao_obito_excluido,

    --column: cod_ibge_munic_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,7))
        END AS STRING
    ) AS id_municipio_certidao_obito_excluido,

    --column: cod_livro_termo_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,239,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,239,8))
        END AS STRING
    ) AS livro_certidao_obito_excluido,

    --column: cod_termo_matricula_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,251,32), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,251,32))
        END AS STRING
    ) AS numero_termo_matricula_certidao_excluido,

    --column: cpf_oper_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,11))
        END AS STRING
    ) AS cpf_operador_exclusao_membro,

    --column: cpf_servd_pbco_pgmcu_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,149,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,149,11))
        END AS STRING
    ) AS cpf_servidor_parecer_gestao_municipal_cadunico_membro,

    --column: dat_emi_pgmcu_mbo
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,8))
        END    ) AS data_emissao_parecer_gestao_municipal_cadunico_membro,

    --column: data_exc_mbo
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_exclusao_membro,

    --column: desc_mot_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,335,255), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,335,255))
        END AS STRING
    ) AS descricao_cotivo_exclusao,

    --column: dta_emissao_certid_mbo_exc
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,283,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,283,8))
        END    ) AS data_emissao_certidao_obito_excluido,

    --column: motivo_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_motivo_exclusao_membro,
    --column: motivo_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^01$') THEN 'Falecimento Da Pessoa'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^02$') THEN 'Desligamento Da Pessoa Daquela Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^03$') THEN 'Solicitação Da Pessoa'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^04$') THEN 'Decisão Judicial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^05$') THEN 'Decurso De Prazo No Estado Cadastral “Em Cadastramento”'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^06$') THEN 'Mudança De Endereço Ocorrido Anteriormente A V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^07$') THEN 'Cadastramento Incorreto Ocorrido Anteriormente A V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^08$') THEN 'Pessoa Transferida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^09$') THEN 'Cadastro Desatualizado Há Mais De 48 Meses'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^10$') THEN 'Averiguação Cadastral'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^11$') THEN 'Multiplicidade'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^12$') THEN 'NIS Cancelado No Cadastro NIS'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^13$') THEN 'Averiguação - Suspeita De Fraude'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^14$') THEN 'Pessoa Excluída Por Decurso De Prazo Na Situação "Atribuindo NIS"'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^15$') THEN 'Averiguação - Suspeita De Fraude Identificada Pelo Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^16$') THEN 'Pessoa Excluída Por Possuir CPF Nulo'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^17$') THEN 'Membro Excluído Por Ter Sido Incluído Pelo Próprio Operador/Entrevistador Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^18$') THEN 'Identificação De Cadastros Incluídos Ou Alterados Indevidamente Por Agente Público Por Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^19$') THEN 'Indicativo De Óbito Há Mais De 12 Meses - Exclusão Batch'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^20$') THEN 'Confirmação De Óbito Pelo RF Via Aplicativo'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^21$') THEN 'Indicativo De Óbito - Bases Do Governo Federal'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^99$') THEN 'Exclusão Da Família (Para Casos Em Que A Pessoa Tenha Sido Excluída Pela Exclusão De Sua Família)'
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS motivo_exclusao_membro,

    --column: mun_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,162,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,162,7))
        END AS STRING
    ) AS id_municipio_parecer_gestao_municipal_cadunico_membro,

    --column: nom_cartorio_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,169,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,169,70))
        END AS STRING
    ) AS cartorio_certidao_obito_excluido,

    --column: nom_munic_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,293,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,293,35))
        END AS STRING
    ) AS municipio_certidao_obito_excluido,

    --column: nom_servd_pbco_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,70))
        END AS STRING
    ) AS servidor_parecer_gestao_municipal_cadunico_membro,

    --column: num_membro_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_excluido,

    --column: num_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,10))
        END AS STRING
    ) AS numero_parecer_gestao_municipal_cadunico_membro,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,291,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,291,2))
        END AS STRING
    ) AS sigla_uf_certidao_obito_excluido,

    --column: uf_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,160,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,160,2))
        END AS STRING
    ) AS sigla_uf_parecer_gestao_municipal_cadunico_membro,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0601'
    AND SUBSTRING(text,38,2) = '19'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_membro_excluido,

    --column: cod_familiar_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_membro_excluido,

    --column: cod_folha_termo_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,247,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,247,4))
        END AS STRING
    ) AS folha_ceritao_obito_excluido,

    --column: cod_ibge_munic_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,7))
        END AS STRING
    ) AS id_municipio_certidao_obito_excluido,

    --column: cod_livro_termo_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,239,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,239,8))
        END AS STRING
    ) AS livro_certidao_obito_excluido,

    --column: cod_termo_matricula_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,251,32), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,251,32))
        END AS STRING
    ) AS numero_termo_matricula_certidao_excluido,

    --column: cpf_oper_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,11))
        END AS STRING
    ) AS cpf_operador_exclusao_membro,

    --column: cpf_servd_pbco_pgmcu_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,149,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,149,11))
        END AS STRING
    ) AS cpf_servidor_parecer_gestao_municipal_cadunico_membro,

    --column: dat_emi_pgmcu_mbo
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,8))
        END    ) AS data_emissao_parecer_gestao_municipal_cadunico_membro,

    --column: data_exc_mbo
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_exclusao_membro,

    --column: desc_mot_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,335,255), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,335,255))
        END AS STRING
    ) AS descricao_cotivo_exclusao,

    --column: dta_emissao_certid_mbo_exc
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,283,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,283,8))
        END    ) AS data_emissao_certidao_obito_excluido,

    --column: motivo_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_motivo_exclusao_membro,
    --column: motivo_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^01$') THEN 'Falecimento Da Pessoa'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^02$') THEN 'Desligamento Da Pessoa Daquela Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^03$') THEN 'Solicitação Da Pessoa'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^04$') THEN 'Decisão Judicial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^05$') THEN 'Decurso De Prazo No Estado Cadastral “Em Cadastramento”'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^06$') THEN 'Mudança De Endereço Ocorrido Anteriormente A V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^07$') THEN 'Cadastramento Incorreto Ocorrido Anteriormente A V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^08$') THEN 'Pessoa Transferida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^09$') THEN 'Cadastro Desatualizado Há Mais De 48 Meses'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^10$') THEN 'Averiguação Cadastral'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^11$') THEN 'Multiplicidade'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^12$') THEN 'NIS Cancelado No Cadastro NIS'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^13$') THEN 'Averiguação - Suspeita De Fraude'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^14$') THEN 'Pessoa Excluída Por Decurso De Prazo Na Situação "Atribuindo NIS"'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^15$') THEN 'Averiguação - Suspeita De Fraude Identificada Pelo Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^16$') THEN 'Pessoa Excluída Por Possuir CPF Nulo'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^17$') THEN 'Membro Excluído Por Ter Sido Incluído Pelo Próprio Operador/Entrevistador Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^18$') THEN 'Identificação De Cadastros Incluídos Ou Alterados Indevidamente Por Agente Público Por Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^19$') THEN 'Indicativo De Óbito Há Mais De 12 Meses - Exclusão Batch'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^20$') THEN 'Confirmação De Óbito Pelo RF Via Aplicativo'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^21$') THEN 'Indicativo De Óbito - Bases Do Governo Federal'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^99$') THEN 'Exclusão Da Família (Para Casos Em Que A Pessoa Tenha Sido Excluída Pela Exclusão De Sua Família)'
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS motivo_exclusao_membro,

    --column: mun_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,162,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,162,7))
        END AS STRING
    ) AS id_municipio_parecer_gestao_municipal_cadunico_membro,

    --column: nom_cartorio_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,169,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,169,70))
        END AS STRING
    ) AS cartorio_certidao_obito_excluido,

    --column: nom_munic_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,293,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,293,35))
        END AS STRING
    ) AS municipio_certidao_obito_excluido,

    --column: nom_servd_pbco_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,70))
        END AS STRING
    ) AS servidor_parecer_gestao_municipal_cadunico_membro,

    --column: num_membro_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_excluido,

    --column: num_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,10))
        END AS STRING
    ) AS numero_parecer_gestao_municipal_cadunico_membro,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,291,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,291,2))
        END AS STRING
    ) AS sigla_uf_certidao_obito_excluido,

    --column: uf_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,160,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,160,2))
        END AS STRING
    ) AS sigla_uf_parecer_gestao_municipal_cadunico_membro,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0603'
    AND SUBSTRING(text,38,2) = '19'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_membro_excluido,

    --column: cod_familiar_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_membro_excluido,

    --column: cod_folha_termo_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,247,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,247,4))
        END AS STRING
    ) AS folha_ceritao_obito_excluido,

    --column: cod_ibge_munic_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,7))
        END AS STRING
    ) AS id_municipio_certidao_obito_excluido,

    --column: cod_livro_termo_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,239,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,239,8))
        END AS STRING
    ) AS livro_certidao_obito_excluido,

    --column: cod_termo_matricula_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,251,32), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,251,32))
        END AS STRING
    ) AS numero_termo_matricula_certidao_excluido,

    --column: cpf_oper_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,11))
        END AS STRING
    ) AS cpf_operador_exclusao_membro,

    --column: cpf_servd_pbco_pgmcu_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,149,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,149,11))
        END AS STRING
    ) AS cpf_servidor_parecer_gestao_municipal_cadunico_membro,

    --column: dat_emi_pgmcu_mbo
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,8))
        END    ) AS data_emissao_parecer_gestao_municipal_cadunico_membro,

    --column: data_exc_mbo
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_exclusao_membro,

    --column: desc_mot_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,335,255), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,335,255))
        END AS STRING
    ) AS descricao_cotivo_exclusao,

    --column: dta_emissao_certid_mbo_exc
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,283,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,283,8))
        END    ) AS data_emissao_certidao_obito_excluido,

    --column: motivo_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_motivo_exclusao_membro,
    --column: motivo_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^01$') THEN 'Falecimento Da Pessoa'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^02$') THEN 'Desligamento Da Pessoa Daquela Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^03$') THEN 'Solicitação Da Pessoa'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^04$') THEN 'Decisão Judicial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^05$') THEN 'Decurso De Prazo No Estado Cadastral “Em Cadastramento”'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^06$') THEN 'Mudança De Endereço Ocorrido Anteriormente A V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^07$') THEN 'Cadastramento Incorreto Ocorrido Anteriormente A V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^08$') THEN 'Pessoa Transferida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^09$') THEN 'Cadastro Desatualizado Há Mais De 48 Meses'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^10$') THEN 'Averiguação Cadastral'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^11$') THEN 'Multiplicidade'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^12$') THEN 'NIS Cancelado No Cadastro NIS'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^13$') THEN 'Averiguação - Suspeita De Fraude'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^14$') THEN 'Pessoa Excluída Por Decurso De Prazo Na Situação "Atribuindo NIS"'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^15$') THEN 'Averiguação - Suspeita De Fraude Identificada Pelo Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^16$') THEN 'Pessoa Excluída Por Possuir CPF Nulo'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^17$') THEN 'Membro Excluído Por Ter Sido Incluído Pelo Próprio Operador/Entrevistador Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^18$') THEN 'Identificação De Cadastros Incluídos Ou Alterados Indevidamente Por Agente Público Por Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^19$') THEN 'Indicativo De Óbito Há Mais De 12 Meses - Exclusão Batch'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^20$') THEN 'Confirmação De Óbito Pelo RF Via Aplicativo'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^21$') THEN 'Indicativo De Óbito - Bases Do Governo Federal'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^99$') THEN 'Exclusão Da Família (Para Casos Em Que A Pessoa Tenha Sido Excluída Pela Exclusão De Sua Família)'
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS motivo_exclusao_membro,

    --column: mun_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,162,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,162,7))
        END AS STRING
    ) AS id_municipio_parecer_gestao_municipal_cadunico_membro,

    --column: nom_cartorio_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,169,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,169,70))
        END AS STRING
    ) AS cartorio_certidao_obito_excluido,

    --column: nom_munic_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,293,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,293,35))
        END AS STRING
    ) AS municipio_certidao_obito_excluido,

    --column: nom_servd_pbco_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,70))
        END AS STRING
    ) AS servidor_parecer_gestao_municipal_cadunico_membro,

    --column: num_membro_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_excluido,

    --column: num_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,10))
        END AS STRING
    ) AS numero_parecer_gestao_municipal_cadunico_membro,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,291,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,291,2))
        END AS STRING
    ) AS sigla_uf_certidao_obito_excluido,

    --column: uf_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,160,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,160,2))
        END AS STRING
    ) AS sigla_uf_parecer_gestao_municipal_cadunico_membro,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0604'
    AND SUBSTRING(text,38,2) = '19'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_membro_excluido,

    --column: cod_familiar_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_membro_excluido,

    --column: cod_folha_termo_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,247,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,247,4))
        END AS STRING
    ) AS folha_ceritao_obito_excluido,

    --column: cod_ibge_munic_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,7))
        END AS STRING
    ) AS id_municipio_certidao_obito_excluido,

    --column: cod_livro_termo_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,239,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,239,8))
        END AS STRING
    ) AS livro_certidao_obito_excluido,

    --column: cod_termo_matricula_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,251,32), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,251,32))
        END AS STRING
    ) AS numero_termo_matricula_certidao_excluido,

    --column: cpf_oper_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,11))
        END AS STRING
    ) AS cpf_operador_exclusao_membro,

    --column: cpf_servd_pbco_pgmcu_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,149,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,149,11))
        END AS STRING
    ) AS cpf_servidor_parecer_gestao_municipal_cadunico_membro,

    --column: dat_emi_pgmcu_mbo
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,8))
        END    ) AS data_emissao_parecer_gestao_municipal_cadunico_membro,

    --column: data_exc_mbo
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_exclusao_membro,

    --column: desc_mot_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,335,255), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,335,255))
        END AS STRING
    ) AS descricao_cotivo_exclusao,

    --column: dta_emissao_certid_mbo_exc
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,283,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,283,8))
        END    ) AS data_emissao_certidao_obito_excluido,

    --column: motivo_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_motivo_exclusao_membro,
    --column: motivo_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^01$') THEN 'Falecimento Da Pessoa'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^02$') THEN 'Desligamento Da Pessoa Daquela Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^03$') THEN 'Solicitação Da Pessoa'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^04$') THEN 'Decisão Judicial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^05$') THEN 'Decurso De Prazo No Estado Cadastral “Em Cadastramento”'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^06$') THEN 'Mudança De Endereço Ocorrido Anteriormente A V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^07$') THEN 'Cadastramento Incorreto Ocorrido Anteriormente A V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^08$') THEN 'Pessoa Transferida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^09$') THEN 'Cadastro Desatualizado Há Mais De 48 Meses'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^10$') THEN 'Averiguação Cadastral'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^11$') THEN 'Multiplicidade'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^12$') THEN 'NIS Cancelado No Cadastro NIS'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^13$') THEN 'Averiguação - Suspeita De Fraude'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^14$') THEN 'Pessoa Excluída Por Decurso De Prazo Na Situação "Atribuindo NIS"'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^15$') THEN 'Averiguação - Suspeita De Fraude Identificada Pelo Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^16$') THEN 'Pessoa Excluída Por Possuir CPF Nulo'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^17$') THEN 'Membro Excluído Por Ter Sido Incluído Pelo Próprio Operador/Entrevistador Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^18$') THEN 'Identificação De Cadastros Incluídos Ou Alterados Indevidamente Por Agente Público Por Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^19$') THEN 'Indicativo De Óbito Há Mais De 12 Meses - Exclusão Batch'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^20$') THEN 'Confirmação De Óbito Pelo RF Via Aplicativo'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^21$') THEN 'Indicativo De Óbito - Bases Do Governo Federal'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^99$') THEN 'Exclusão Da Família (Para Casos Em Que A Pessoa Tenha Sido Excluída Pela Exclusão De Sua Família)'
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS motivo_exclusao_membro,

    --column: mun_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,162,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,162,7))
        END AS STRING
    ) AS id_municipio_parecer_gestao_municipal_cadunico_membro,

    --column: nom_cartorio_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,169,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,169,70))
        END AS STRING
    ) AS cartorio_certidao_obito_excluido,

    --column: nom_munic_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,293,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,293,35))
        END AS STRING
    ) AS municipio_certidao_obito_excluido,

    --column: nom_servd_pbco_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,70))
        END AS STRING
    ) AS servidor_parecer_gestao_municipal_cadunico_membro,

    --column: num_membro_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_excluido,

    --column: num_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,10))
        END AS STRING
    ) AS numero_parecer_gestao_municipal_cadunico_membro,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,291,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,291,2))
        END AS STRING
    ) AS sigla_uf_certidao_obito_excluido,

    --column: uf_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,160,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,160,2))
        END AS STRING
    ) AS sigla_uf_parecer_gestao_municipal_cadunico_membro,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0609'
    AND SUBSTRING(text,38,2) = '19'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_membro_excluido,

    --column: cod_familiar_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_membro_excluido,

    --column: cod_folha_termo_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,247,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,247,4))
        END AS STRING
    ) AS folha_ceritao_obito_excluido,

    --column: cod_ibge_munic_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,7))
        END AS STRING
    ) AS id_municipio_certidao_obito_excluido,

    --column: cod_livro_termo_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,239,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,239,8))
        END AS STRING
    ) AS livro_certidao_obito_excluido,

    --column: cod_termo_matricula_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,251,32), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,251,32))
        END AS STRING
    ) AS numero_termo_matricula_certidao_excluido,

    --column: cpf_oper_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,11))
        END AS STRING
    ) AS cpf_operador_exclusao_membro,

    --column: cpf_servd_pbco_pgmcu_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,149,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,149,11))
        END AS STRING
    ) AS cpf_servidor_parecer_gestao_municipal_cadunico_membro,

    --column: dat_emi_pgmcu_mbo
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,8))
        END    ) AS data_emissao_parecer_gestao_municipal_cadunico_membro,

    --column: data_exc_mbo
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_exclusao_membro,

    --column: desc_mot_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,335,255), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,335,255))
        END AS STRING
    ) AS descricao_cotivo_exclusao,

    --column: dta_emissao_certid_mbo_exc
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,283,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,283,8))
        END    ) AS data_emissao_certidao_obito_excluido,

    --column: motivo_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_motivo_exclusao_membro,
    --column: motivo_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^01$') THEN 'Falecimento Da Pessoa'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^02$') THEN 'Desligamento Da Pessoa Daquela Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^03$') THEN 'Solicitação Da Pessoa'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^04$') THEN 'Decisão Judicial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^05$') THEN 'Decurso De Prazo No Estado Cadastral “Em Cadastramento”'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^06$') THEN 'Mudança De Endereço Ocorrido Anteriormente A V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^07$') THEN 'Cadastramento Incorreto Ocorrido Anteriormente A V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^08$') THEN 'Pessoa Transferida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^09$') THEN 'Cadastro Desatualizado Há Mais De 48 Meses'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^10$') THEN 'Averiguação Cadastral'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^11$') THEN 'Multiplicidade'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^12$') THEN 'NIS Cancelado No Cadastro NIS'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^13$') THEN 'Averiguação - Suspeita De Fraude'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^14$') THEN 'Pessoa Excluída Por Decurso De Prazo Na Situação "Atribuindo NIS"'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^15$') THEN 'Averiguação - Suspeita De Fraude Identificada Pelo Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^16$') THEN 'Pessoa Excluída Por Possuir CPF Nulo'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^17$') THEN 'Membro Excluído Por Ter Sido Incluído Pelo Próprio Operador/Entrevistador Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^18$') THEN 'Identificação De Cadastros Incluídos Ou Alterados Indevidamente Por Agente Público Por Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^19$') THEN 'Indicativo De Óbito Há Mais De 12 Meses - Exclusão Batch'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^20$') THEN 'Confirmação De Óbito Pelo RF Via Aplicativo'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^21$') THEN 'Indicativo De Óbito - Bases Do Governo Federal'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^99$') THEN 'Exclusão Da Família (Para Casos Em Que A Pessoa Tenha Sido Excluída Pela Exclusão De Sua Família)'
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS motivo_exclusao_membro,

    --column: mun_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,162,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,162,7))
        END AS STRING
    ) AS id_municipio_parecer_gestao_municipal_cadunico_membro,

    --column: nom_cartorio_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,169,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,169,70))
        END AS STRING
    ) AS cartorio_certidao_obito_excluido,

    --column: nom_munic_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,293,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,293,35))
        END AS STRING
    ) AS municipio_certidao_obito_excluido,

    --column: nom_servd_pbco_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,70))
        END AS STRING
    ) AS servidor_parecer_gestao_municipal_cadunico_membro,

    --column: num_membro_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_excluido,

    --column: num_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,10))
        END AS STRING
    ) AS numero_parecer_gestao_municipal_cadunico_membro,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,291,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,291,2))
        END AS STRING
    ) AS sigla_uf_certidao_obito_excluido,

    --column: uf_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,160,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,160,2))
        END AS STRING
    ) AS sigla_uf_parecer_gestao_municipal_cadunico_membro,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0612'
    AND SUBSTRING(text,38,2) = '19'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_membro_excluido,

    --column: cod_familiar_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_membro_excluido,

    --column: cod_folha_termo_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,247,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,247,4))
        END AS STRING
    ) AS folha_ceritao_obito_excluido,

    --column: cod_ibge_munic_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,7))
        END AS STRING
    ) AS id_municipio_certidao_obito_excluido,

    --column: cod_livro_termo_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,239,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,239,8))
        END AS STRING
    ) AS livro_certidao_obito_excluido,

    --column: cod_termo_matricula_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,251,32), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,251,32))
        END AS STRING
    ) AS numero_termo_matricula_certidao_excluido,

    --column: cpf_oper_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,11))
        END AS STRING
    ) AS cpf_operador_exclusao_membro,

    --column: cpf_servd_pbco_pgmcu_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,149,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,149,11))
        END AS STRING
    ) AS cpf_servidor_parecer_gestao_municipal_cadunico_membro,

    --column: dat_emi_pgmcu_mbo
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,8))
        END    ) AS data_emissao_parecer_gestao_municipal_cadunico_membro,

    --column: data_exc_mbo
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_exclusao_membro,

    --column: desc_mot_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,335,255), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,335,255))
        END AS STRING
    ) AS descricao_cotivo_exclusao,

    --column: dta_emissao_certid_mbo_exc
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,283,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,283,8))
        END    ) AS data_emissao_certidao_obito_excluido,

    --column: motivo_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_motivo_exclusao_membro,
    --column: motivo_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^01$') THEN 'Falecimento Da Pessoa'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^02$') THEN 'Desligamento Da Pessoa Daquela Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^03$') THEN 'Solicitação Da Pessoa'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^04$') THEN 'Decisão Judicial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^05$') THEN 'Decurso De Prazo No Estado Cadastral “Em Cadastramento”'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^06$') THEN 'Mudança De Endereço Ocorrido Anteriormente A V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^07$') THEN 'Cadastramento Incorreto Ocorrido Anteriormente A V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^08$') THEN 'Pessoa Transferida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^09$') THEN 'Cadastro Desatualizado Há Mais De 48 Meses'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^10$') THEN 'Averiguação Cadastral'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^11$') THEN 'Multiplicidade'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^12$') THEN 'NIS Cancelado No Cadastro NIS'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^13$') THEN 'Averiguação - Suspeita De Fraude'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^14$') THEN 'Pessoa Excluída Por Decurso De Prazo Na Situação "Atribuindo NIS"'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^15$') THEN 'Averiguação - Suspeita De Fraude Identificada Pelo Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^16$') THEN 'Pessoa Excluída Por Possuir CPF Nulo'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^17$') THEN 'Membro Excluído Por Ter Sido Incluído Pelo Próprio Operador/Entrevistador Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^18$') THEN 'Identificação De Cadastros Incluídos Ou Alterados Indevidamente Por Agente Público Por Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^19$') THEN 'Indicativo De Óbito Há Mais De 12 Meses - Exclusão Batch'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^20$') THEN 'Confirmação De Óbito Pelo RF Via Aplicativo'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^21$') THEN 'Indicativo De Óbito - Bases Do Governo Federal'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^99$') THEN 'Exclusão Da Família (Para Casos Em Que A Pessoa Tenha Sido Excluída Pela Exclusão De Sua Família)'
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS motivo_exclusao_membro,

    --column: mun_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,162,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,162,7))
        END AS STRING
    ) AS id_municipio_parecer_gestao_municipal_cadunico_membro,

    --column: nom_cartorio_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,169,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,169,70))
        END AS STRING
    ) AS cartorio_certidao_obito_excluido,

    --column: nom_munic_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,293,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,293,35))
        END AS STRING
    ) AS municipio_certidao_obito_excluido,

    --column: nom_servd_pbco_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,70))
        END AS STRING
    ) AS servidor_parecer_gestao_municipal_cadunico_membro,

    --column: num_membro_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_excluido,

    --column: num_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,10))
        END AS STRING
    ) AS numero_parecer_gestao_municipal_cadunico_membro,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,291,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,291,2))
        END AS STRING
    ) AS sigla_uf_certidao_obito_excluido,

    --column: uf_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,160,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,160,2))
        END AS STRING
    ) AS sigla_uf_parecer_gestao_municipal_cadunico_membro,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0615'
    AND SUBSTRING(text,38,2) = '19'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_membro_excluido,

    --column: cod_familiar_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_membro_excluido,

    --column: cod_folha_termo_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,247,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,247,4))
        END AS STRING
    ) AS folha_ceritao_obito_excluido,

    --column: cod_ibge_munic_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,7))
        END AS STRING
    ) AS id_municipio_certidao_obito_excluido,

    --column: cod_livro_termo_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,239,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,239,8))
        END AS STRING
    ) AS livro_certidao_obito_excluido,

    --column: cod_termo_matricula_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,251,32), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,251,32))
        END AS STRING
    ) AS numero_termo_matricula_certidao_excluido,

    --column: cpf_oper_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,11))
        END AS STRING
    ) AS cpf_operador_exclusao_membro,

    --column: cpf_servd_pbco_pgmcu_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,149,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,149,11))
        END AS STRING
    ) AS cpf_servidor_parecer_gestao_municipal_cadunico_membro,

    --column: dat_emi_pgmcu_mbo
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,8))
        END    ) AS data_emissao_parecer_gestao_municipal_cadunico_membro,

    --column: data_exc_mbo
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_exclusao_membro,

    --column: desc_mot_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,335,255), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,335,255))
        END AS STRING
    ) AS descricao_cotivo_exclusao,

    --column: dta_emissao_certid_mbo_exc
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,283,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,283,8))
        END    ) AS data_emissao_certidao_obito_excluido,

    --column: motivo_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_motivo_exclusao_membro,
    --column: motivo_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^01$') THEN 'Falecimento Da Pessoa'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^02$') THEN 'Desligamento Da Pessoa Daquela Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^03$') THEN 'Solicitação Da Pessoa'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^04$') THEN 'Decisão Judicial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^05$') THEN 'Decurso De Prazo No Estado Cadastral “Em Cadastramento”'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^06$') THEN 'Mudança De Endereço Ocorrido Anteriormente A V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^07$') THEN 'Cadastramento Incorreto Ocorrido Anteriormente A V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^08$') THEN 'Pessoa Transferida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^09$') THEN 'Cadastro Desatualizado Há Mais De 48 Meses'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^10$') THEN 'Averiguação Cadastral'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^11$') THEN 'Multiplicidade'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^12$') THEN 'NIS Cancelado No Cadastro NIS'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^13$') THEN 'Averiguação - Suspeita De Fraude'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^14$') THEN 'Pessoa Excluída Por Decurso De Prazo Na Situação "Atribuindo NIS"'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^15$') THEN 'Averiguação - Suspeita De Fraude Identificada Pelo Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^16$') THEN 'Pessoa Excluída Por Possuir CPF Nulo'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^17$') THEN 'Membro Excluído Por Ter Sido Incluído Pelo Próprio Operador/Entrevistador Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^18$') THEN 'Identificação De Cadastros Incluídos Ou Alterados Indevidamente Por Agente Público Por Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^19$') THEN 'Indicativo De Óbito Há Mais De 12 Meses - Exclusão Batch'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^20$') THEN 'Confirmação De Óbito Pelo RF Via Aplicativo'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^21$') THEN 'Indicativo De Óbito - Bases Do Governo Federal'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^99$') THEN 'Exclusão Da Família (Para Casos Em Que A Pessoa Tenha Sido Excluída Pela Exclusão De Sua Família)'
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS motivo_exclusao_membro,

    --column: mun_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,162,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,162,7))
        END AS STRING
    ) AS id_municipio_parecer_gestao_municipal_cadunico_membro,

    --column: nom_cartorio_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,169,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,169,70))
        END AS STRING
    ) AS cartorio_certidao_obito_excluido,

    --column: nom_munic_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,293,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,293,35))
        END AS STRING
    ) AS municipio_certidao_obito_excluido,

    --column: nom_servd_pbco_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,70))
        END AS STRING
    ) AS servidor_parecer_gestao_municipal_cadunico_membro,

    --column: num_membro_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_excluido,

    --column: num_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,10))
        END AS STRING
    ) AS numero_parecer_gestao_municipal_cadunico_membro,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,291,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,291,2))
        END AS STRING
    ) AS sigla_uf_certidao_obito_excluido,

    --column: uf_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,160,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,160,2))
        END AS STRING
    ) AS sigla_uf_parecer_gestao_municipal_cadunico_membro,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0617'
    AND SUBSTRING(text,38,2) = '19'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_membro_excluido,

    --column: cod_familiar_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_membro_excluido,

    --column: cod_folha_termo_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,247,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,247,4))
        END AS STRING
    ) AS folha_ceritao_obito_excluido,

    --column: cod_ibge_munic_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,7))
        END AS STRING
    ) AS id_municipio_certidao_obito_excluido,

    --column: cod_livro_termo_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,239,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,239,8))
        END AS STRING
    ) AS livro_certidao_obito_excluido,

    --column: cod_termo_matricula_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,251,32), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,251,32))
        END AS STRING
    ) AS numero_termo_matricula_certidao_excluido,

    --column: cpf_oper_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,11))
        END AS STRING
    ) AS cpf_operador_exclusao_membro,

    --column: cpf_servd_pbco_pgmcu_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,149,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,149,11))
        END AS STRING
    ) AS cpf_servidor_parecer_gestao_municipal_cadunico_membro,

    --column: dat_emi_pgmcu_mbo
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,8))
        END    ) AS data_emissao_parecer_gestao_municipal_cadunico_membro,

    --column: data_exc_mbo
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_exclusao_membro,

    --column: desc_mot_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,335,255), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,335,255))
        END AS STRING
    ) AS descricao_cotivo_exclusao,

    --column: dta_emissao_certid_mbo_exc
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,283,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,283,8))
        END    ) AS data_emissao_certidao_obito_excluido,

    --column: motivo_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_motivo_exclusao_membro,
    --column: motivo_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^01$') THEN 'Falecimento Da Pessoa'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^02$') THEN 'Desligamento Da Pessoa Daquela Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^03$') THEN 'Solicitação Da Pessoa'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^04$') THEN 'Decisão Judicial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^05$') THEN 'Decurso De Prazo No Estado Cadastral “Em Cadastramento”'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^06$') THEN 'Mudança De Endereço Ocorrido Anteriormente A V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^07$') THEN 'Cadastramento Incorreto Ocorrido Anteriormente A V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^08$') THEN 'Pessoa Transferida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^09$') THEN 'Cadastro Desatualizado Há Mais De 48 Meses'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^10$') THEN 'Averiguação Cadastral'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^11$') THEN 'Multiplicidade'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^12$') THEN 'NIS Cancelado No Cadastro NIS'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^13$') THEN 'Averiguação - Suspeita De Fraude'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^14$') THEN 'Pessoa Excluída Por Decurso De Prazo Na Situação "Atribuindo NIS"'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^15$') THEN 'Averiguação - Suspeita De Fraude Identificada Pelo Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^16$') THEN 'Pessoa Excluída Por Possuir CPF Nulo'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^17$') THEN 'Membro Excluído Por Ter Sido Incluído Pelo Próprio Operador/Entrevistador Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^18$') THEN 'Identificação De Cadastros Incluídos Ou Alterados Indevidamente Por Agente Público Por Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^19$') THEN 'Indicativo De Óbito Há Mais De 12 Meses - Exclusão Batch'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^20$') THEN 'Confirmação De Óbito Pelo RF Via Aplicativo'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^21$') THEN 'Indicativo De Óbito - Bases Do Governo Federal'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^99$') THEN 'Exclusão Da Família (Para Casos Em Que A Pessoa Tenha Sido Excluída Pela Exclusão De Sua Família)'
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS motivo_exclusao_membro,

    --column: mun_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,162,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,162,7))
        END AS STRING
    ) AS id_municipio_parecer_gestao_municipal_cadunico_membro,

    --column: nom_cartorio_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,169,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,169,70))
        END AS STRING
    ) AS cartorio_certidao_obito_excluido,

    --column: nom_munic_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,293,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,293,35))
        END AS STRING
    ) AS municipio_certidao_obito_excluido,

    --column: nom_servd_pbco_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,70))
        END AS STRING
    ) AS servidor_parecer_gestao_municipal_cadunico_membro,

    --column: num_membro_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_excluido,

    --column: num_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,10))
        END AS STRING
    ) AS numero_parecer_gestao_municipal_cadunico_membro,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,291,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,291,2))
        END AS STRING
    ) AS sigla_uf_certidao_obito_excluido,

    --column: uf_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,160,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,160,2))
        END AS STRING
    ) AS sigla_uf_parecer_gestao_municipal_cadunico_membro,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0619'
    AND SUBSTRING(text,38,2) = '19'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_membro_excluido,

    --column: cod_familiar_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_membro_excluido,

    --column: cod_folha_termo_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,247,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,247,4))
        END AS STRING
    ) AS folha_ceritao_obito_excluido,

    --column: cod_ibge_munic_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,7))
        END AS STRING
    ) AS id_municipio_certidao_obito_excluido,

    --column: cod_livro_termo_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,239,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,239,8))
        END AS STRING
    ) AS livro_certidao_obito_excluido,

    --column: cod_termo_matricula_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,251,32), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,251,32))
        END AS STRING
    ) AS numero_termo_matricula_certidao_excluido,

    --column: cpf_oper_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,11))
        END AS STRING
    ) AS cpf_operador_exclusao_membro,

    --column: cpf_servd_pbco_pgmcu_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,149,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,149,11))
        END AS STRING
    ) AS cpf_servidor_parecer_gestao_municipal_cadunico_membro,

    --column: dat_emi_pgmcu_mbo
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,8))
        END    ) AS data_emissao_parecer_gestao_municipal_cadunico_membro,

    --column: data_exc_mbo
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_exclusao_membro,

    --column: desc_mot_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,335,255), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,335,255))
        END AS STRING
    ) AS descricao_cotivo_exclusao,

    --column: dta_emissao_certid_mbo_exc
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,283,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,283,8))
        END    ) AS data_emissao_certidao_obito_excluido,

    --column: motivo_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_motivo_exclusao_membro,
    --column: motivo_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^01$') THEN 'Falecimento Da Pessoa'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^02$') THEN 'Desligamento Da Pessoa Daquela Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^03$') THEN 'Solicitação Da Pessoa'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^04$') THEN 'Decisão Judicial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^05$') THEN 'Decurso De Prazo No Estado Cadastral “Em Cadastramento”'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^06$') THEN 'Mudança De Endereço Ocorrido Anteriormente A V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^07$') THEN 'Cadastramento Incorreto Ocorrido Anteriormente A V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^08$') THEN 'Pessoa Transferida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^09$') THEN 'Cadastro Desatualizado Há Mais De 48 Meses'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^10$') THEN 'Averiguação Cadastral'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^11$') THEN 'Multiplicidade'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^12$') THEN 'NIS Cancelado No Cadastro NIS'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^13$') THEN 'Averiguação - Suspeita De Fraude'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^14$') THEN 'Pessoa Excluída Por Decurso De Prazo Na Situação "Atribuindo NIS"'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^15$') THEN 'Averiguação - Suspeita De Fraude Identificada Pelo Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^16$') THEN 'Pessoa Excluída Por Possuir CPF Nulo'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^17$') THEN 'Membro Excluído Por Ter Sido Incluído Pelo Próprio Operador/Entrevistador Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^18$') THEN 'Identificação De Cadastros Incluídos Ou Alterados Indevidamente Por Agente Público Por Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^19$') THEN 'Indicativo De Óbito Há Mais De 12 Meses - Exclusão Batch'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^20$') THEN 'Confirmação De Óbito Pelo RF Via Aplicativo'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^21$') THEN 'Indicativo De Óbito - Bases Do Governo Federal'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^99$') THEN 'Exclusão Da Família (Para Casos Em Que A Pessoa Tenha Sido Excluída Pela Exclusão De Sua Família)'
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS motivo_exclusao_membro,

    --column: mun_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,162,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,162,7))
        END AS STRING
    ) AS id_municipio_parecer_gestao_municipal_cadunico_membro,

    --column: nom_cartorio_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,169,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,169,70))
        END AS STRING
    ) AS cartorio_certidao_obito_excluido,

    --column: nom_munic_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,293,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,293,35))
        END AS STRING
    ) AS municipio_certidao_obito_excluido,

    --column: nom_servd_pbco_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,70))
        END AS STRING
    ) AS servidor_parecer_gestao_municipal_cadunico_membro,

    --column: num_membro_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_excluido,

    --column: num_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,10))
        END AS STRING
    ) AS numero_parecer_gestao_municipal_cadunico_membro,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,291,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,291,2))
        END AS STRING
    ) AS sigla_uf_certidao_obito_excluido,

    --column: uf_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,160,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,160,2))
        END AS STRING
    ) AS sigla_uf_parecer_gestao_municipal_cadunico_membro,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0620'
    AND SUBSTRING(text,38,2) = '19'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_membro_excluido,

    --column: cod_familiar_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_membro_excluido,

    --column: cod_folha_termo_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,247,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,247,4))
        END AS STRING
    ) AS folha_ceritao_obito_excluido,

    --column: cod_ibge_munic_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,7))
        END AS STRING
    ) AS id_municipio_certidao_obito_excluido,

    --column: cod_livro_termo_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,239,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,239,8))
        END AS STRING
    ) AS livro_certidao_obito_excluido,

    --column: cod_termo_matricula_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,251,32), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,251,32))
        END AS STRING
    ) AS numero_termo_matricula_certidao_excluido,

    --column: cpf_oper_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,11))
        END AS STRING
    ) AS cpf_operador_exclusao_membro,

    --column: cpf_servd_pbco_pgmcu_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,149,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,149,11))
        END AS STRING
    ) AS cpf_servidor_parecer_gestao_municipal_cadunico_membro,

    --column: dat_emi_pgmcu_mbo
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,8))
        END    ) AS data_emissao_parecer_gestao_municipal_cadunico_membro,

    --column: data_exc_mbo
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_exclusao_membro,

    --column: desc_mot_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,335,255), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,335,255))
        END AS STRING
    ) AS descricao_cotivo_exclusao,

    --column: dta_emissao_certid_mbo_exc
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,283,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,283,8))
        END    ) AS data_emissao_certidao_obito_excluido,

    --column: motivo_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_motivo_exclusao_membro,
    --column: motivo_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^01$') THEN 'Falecimento Da Pessoa'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^02$') THEN 'Desligamento Da Pessoa Daquela Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^03$') THEN 'Solicitação Da Pessoa'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^04$') THEN 'Decisão Judicial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^05$') THEN 'Decurso De Prazo No Estado Cadastral “Em Cadastramento”'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^06$') THEN 'Mudança De Endereço Ocorrido Anteriormente A V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^07$') THEN 'Cadastramento Incorreto Ocorrido Anteriormente A V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^08$') THEN 'Pessoa Transferida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^09$') THEN 'Cadastro Desatualizado Há Mais De 48 Meses'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^10$') THEN 'Averiguação Cadastral'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^11$') THEN 'Multiplicidade'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^12$') THEN 'NIS Cancelado No Cadastro NIS'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^13$') THEN 'Averiguação - Suspeita De Fraude'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^14$') THEN 'Pessoa Excluída Por Decurso De Prazo Na Situação "Atribuindo NIS"'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^15$') THEN 'Averiguação - Suspeita De Fraude Identificada Pelo Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^16$') THEN 'Pessoa Excluída Por Possuir CPF Nulo'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^17$') THEN 'Membro Excluído Por Ter Sido Incluído Pelo Próprio Operador/Entrevistador Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^18$') THEN 'Identificação De Cadastros Incluídos Ou Alterados Indevidamente Por Agente Público Por Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^19$') THEN 'Indicativo De Óbito Há Mais De 12 Meses - Exclusão Batch'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^20$') THEN 'Confirmação De Óbito Pelo RF Via Aplicativo'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^21$') THEN 'Indicativo De Óbito - Bases Do Governo Federal'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^99$') THEN 'Exclusão Da Família (Para Casos Em Que A Pessoa Tenha Sido Excluída Pela Exclusão De Sua Família)'
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS motivo_exclusao_membro,

    --column: mun_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,162,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,162,7))
        END AS STRING
    ) AS id_municipio_parecer_gestao_municipal_cadunico_membro,

    --column: nom_cartorio_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,169,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,169,70))
        END AS STRING
    ) AS cartorio_certidao_obito_excluido,

    --column: nom_munic_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,293,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,293,35))
        END AS STRING
    ) AS municipio_certidao_obito_excluido,

    --column: nom_servd_pbco_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,70))
        END AS STRING
    ) AS servidor_parecer_gestao_municipal_cadunico_membro,

    --column: num_membro_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_excluido,

    --column: num_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,10))
        END AS STRING
    ) AS numero_parecer_gestao_municipal_cadunico_membro,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,291,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,291,2))
        END AS STRING
    ) AS sigla_uf_certidao_obito_excluido,

    --column: uf_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,160,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,160,2))
        END AS STRING
    ) AS sigla_uf_parecer_gestao_municipal_cadunico_membro,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0623'
    AND SUBSTRING(text,38,2) = '19'

UNION ALL


SELECT

    --column: chv_natural_prefeitura_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,13), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,1,13))
        END AS STRING
    ) AS id_prefeitura_membro_excluido,

    --column: cod_familiar_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,14,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,14,11))
        END AS STRING
    ) AS id_familia_membro_excluido,

    --column: cod_folha_termo_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,247,4), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,247,4))
        END AS STRING
    ) AS folha_ceritao_obito_excluido,

    --column: cod_ibge_munic_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,328,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,328,7))
        END AS STRING
    ) AS id_municipio_certidao_obito_excluido,

    --column: cod_livro_termo_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,239,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,239,8))
        END AS STRING
    ) AS livro_certidao_obito_excluido,

    --column: cod_termo_matricula_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,251,32), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,251,32))
        END AS STRING
    ) AS numero_termo_matricula_certidao_excluido,

    --column: cpf_oper_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,48,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,48,11))
        END AS STRING
    ) AS cpf_operador_exclusao_membro,

    --column: cpf_servd_pbco_pgmcu_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,149,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,149,11))
        END AS STRING
    ) AS cpf_servidor_parecer_gestao_municipal_cadunico_membro,

    --column: dat_emi_pgmcu_mbo
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,71,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,71,8))
        END    ) AS data_emissao_parecer_gestao_municipal_cadunico_membro,

    --column: data_exc_mbo
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,8))
        END    ) AS data_exclusao_membro,

    --column: desc_mot_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,335,255), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,335,255))
        END AS STRING
    ) AS descricao_cotivo_exclusao,

    --column: dta_emissao_certid_mbo_exc
    SAFE.PARSE_DATE(
        '%d%m%Y',
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,283,8), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,283,8))
        END    ) AS data_emissao_certidao_obito_excluido,

    --column: motivo_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS id_motivo_exclusao_membro,
    --column: motivo_exc_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^\s*$') THEN NULL
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^01$') THEN 'Falecimento Da Pessoa'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^02$') THEN 'Desligamento Da Pessoa Daquela Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^03$') THEN 'Solicitação Da Pessoa'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^04$') THEN 'Decisão Judicial'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^05$') THEN 'Decurso De Prazo No Estado Cadastral “Em Cadastramento”'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^06$') THEN 'Mudança De Endereço Ocorrido Anteriormente A V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^07$') THEN 'Cadastramento Incorreto Ocorrido Anteriormente A V7'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^08$') THEN 'Pessoa Transferida'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^09$') THEN 'Cadastro Desatualizado Há Mais De 48 Meses'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^10$') THEN 'Averiguação Cadastral'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^11$') THEN 'Multiplicidade'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^12$') THEN 'NIS Cancelado No Cadastro NIS'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^13$') THEN 'Averiguação - Suspeita De Fraude'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^14$') THEN 'Pessoa Excluída Por Decurso De Prazo Na Situação "Atribuindo NIS"'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^15$') THEN 'Averiguação - Suspeita De Fraude Identificada Pelo Município'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^16$') THEN 'Pessoa Excluída Por Possuir CPF Nulo'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^17$') THEN 'Membro Excluído Por Ter Sido Incluído Pelo Próprio Operador/Entrevistador Da Família'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^18$') THEN 'Identificação De Cadastros Incluídos Ou Alterados Indevidamente Por Agente Público Por Má Fé'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^19$') THEN 'Indicativo De Óbito Há Mais De 12 Meses - Exclusão Batch'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^20$') THEN 'Confirmação De Óbito Pelo RF Via Aplicativo'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^21$') THEN 'Indicativo De Óbito - Bases Do Governo Federal'
            WHEN REGEXP_CONTAINS(SUBSTRING(text,59,2), r'^99$') THEN 'Exclusão Da Família (Para Casos Em Que A Pessoa Tenha Sido Excluída Pela Exclusão De Sua Família)'
            ELSE TRIM(SUBSTRING(text,59,2))
        END AS STRING
    ) AS motivo_exclusao_membro,

    --column: mun_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,162,7), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,162,7))
        END AS STRING
    ) AS id_municipio_parecer_gestao_municipal_cadunico_membro,

    --column: nom_cartorio_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,169,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,169,70))
        END AS STRING
    ) AS cartorio_certidao_obito_excluido,

    --column: nom_munic_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,293,35), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,293,35))
        END AS STRING
    ) AS municipio_certidao_obito_excluido,

    --column: nom_servd_pbco_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,79,70), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,79,70))
        END AS STRING
    ) AS servidor_parecer_gestao_municipal_cadunico_membro,

    --column: num_membro_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,25,11), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,25,11))
        END AS STRING
    ) AS id_membro_excluido,

    --column: num_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,61,10), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,61,10))
        END AS STRING
    ) AS numero_parecer_gestao_municipal_cadunico_membro,

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: sig_uf_certid_mbo_exc
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,291,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,291,2))
        END AS STRING
    ) AS sigla_uf_certidao_obito_excluido,

    --column: uf_pgmcu_mbo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,160,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,160,2))
        END AS STRING
    ) AS sigla_uf_parecer_gestao_municipal_cadunico_membro,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0624'
    AND SUBSTRING(text,38,2) = '19'

