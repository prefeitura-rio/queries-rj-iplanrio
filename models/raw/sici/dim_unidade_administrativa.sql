with
    unidade_administrativa_staging as (
        select
            ug_cd_ug,
            qt_filhos,
            in_tipologia_ua_d,
            in_tipo_ua_d,
            in_nat_atividade_d,
            ua_type,
            natjur_cd_nat_juridica,
            cd_ua_basica,
            in_classe,
            in_nivel,
            missao_ua
        from `rj-iplanrio.brutos_sici_staging.unidade_administrativa`
    ),
    unidade_administrativa_his_staging as (
        select
            dt_historico,
            ua_ug_cd_ug,
            visao_ua,
            dados_cadastrais,
            atoadm_cd_ato,
            tipmovua_cd_tipo_movimento,
            compet_cd_competencia,
            cgc,
            nr_inscr_municipal,
            tp_ramo_ativ_d,
            in_decisao,
            in_espacializacao,
            in_empresas,
            cd_negocio,
            cd_sistema,
            cd_subsistema,
            in_nivel_estrutura,
            in_sistema_negocio,
            in_atendimento_publico,
            in_atividade_fim_pcrj,
            in_prestacao_servicos,
            in_atividade_fim_ua_basica,
            in_colegiado,
            in_tipo_ua_d,
            in_nivel,
        -- cd_sdi_old,
        -- cd_sdi_reduz_old
        from `rj-iplanrio.brutos_sici_staging.unidade_administrativa_his`
    ),
    t_ug_atual_staging as (
        select
            cd_ug,
            dt_validade,
            cd_ua_pai,
            nm_ug,
            sg_ug,
            -- cd_ato,
            cd_ua_basica,
            ordem,
            ordem_relativa,
            nivel,
            cd_nat_juridica,
            ua_type,
            status,
            ordem_ua_basica
        from `rj-iplanrio.brutos_sici_staging.t_ug_atual`
    ),
    unidade_gestao_his_staging as (
        select
            dt_historico,
            nm_ug,
            sg_ug,
            in_status_ug_d,
            ug_cd_ug,
            tipmovug_cd_tipo_movimento,
            atoadm_cd_ato,
            cd_log,
            nr_porta,
            nm_compl,
            email,
            dados_cadastrais,
            cgc,
            nr_inscr_municipal,
            natjur_cd_nat_juridica,
            nr_inscr_estadual
        from `rj-iplanrio.brutos_sici_staging.unidade_gestao_his`
    ),
    natureza_juridica_staging as (
        select cd_nat_juridica, nm_nat_juridica
        from `rj-iplanrio.brutos_sici_staging.natureza_juridica`
    ),
    tipo_movimento_ua_staging as (
        select cd_tipo_movimento, ds_tipo_movimento, sg_tipo_movimento
        from `rj-iplanrio.brutos_sici_staging.tipo_movimento_ua`
    ),
    competencia_staging as (
        select cd_competencia, tx_competencia
        from `rj-iplanrio.brutos_sici_staging.competencia`
    )

-- Tabela final: recursos_humanos_sici.unidade_administrativa
-- A tabela `unidade_administrativa_his` serve como fato, registrando cada mudança
-- histórica.
-- As demais tabelas são unidas como dimensões para enriquecer os dados.
select
    -- Chaves Primárias
    safe_cast(his.ua_ug_cd_ug as int64) as id_unidade_administrativa,
    safe_cast(atual.cd_ua_pai as int64) as id_unidade_pai,
    safe_cast(ua.cd_ua_basica as int64) as id_unidade_basica,

    -- Atributos da Unidade
    safe_cast(gestao_his.nm_ug as string) as nome,
    safe_cast(gestao_his.sg_ug as string) as sigla,
    safe_cast(ua.ua_type as string) as tipo,
    safe_cast(his.in_nivel as int64) as nivel,
    safe_cast(atual.status as string) as status,

    -- Datas
    safe.parse_date(
        '%Y-%m-%d', substr(cast(atual.dt_validade as string), 1, 10)
    ) as data_validade_registro,
    safe.parse_date(
        '%Y-%m-%d', substr(cast(his.dt_historico as string), 1, 10)
    ) as data_snapshot,  -- Renomeado de data_historico para maior clareza

    -- Chaves Estrangeiras e Descrições
    safe_cast(his.atoadm_cd_ato as int64) as id_ato_administrativo,
    safe_cast(his.tipmovua_cd_tipo_movimento as int64) as id_tipo_movimento,
    safe_cast(tm.ds_tipo_movimento as string) as tipo_movimento,
    safe_cast(ua.natjur_cd_nat_juridica as int64) as id_natureza_juridica,
    safe_cast(nj.nm_nat_juridica as string) as natureza_juridica,
    safe_cast(his.compet_cd_competencia as int64) as id_competencia,
    safe_cast(comp.tx_competencia as string) as competencia,

    -- Atributos da Gestão
    safe_cast(gestao_his.cgc as string) as cnpj,
    safe_cast(gestao_his.email as string) as email,

    -- Indicadores (flags)
    (safe_cast(his.in_colegiado as int64) = 1) as indicador_orgao_colegiado,
    (
        safe_cast(his.in_atendimento_publico as int64) = 1
    ) as indicador_atendimento_publico,
    (safe_cast(his.in_atividade_fim_pcrj as int64) = 1) as indicador_atividade_fim_pcrj,
    (safe_cast(his.in_prestacao_servicos as int64) = 1) as indicador_prestacao_servicos

from unidade_administrativa_his_staging as his
left join unidade_administrativa_staging as ua on his.ua_ug_cd_ug = ua.ug_cd_ug
left join t_ug_atual_staging as atual on his.ua_ug_cd_ug = atual.cd_ug
-- A junção com `unidade_gestao_his` usa a data do histórico para garantir a
-- consistência temporal dos dados de gestão
left join
    unidade_gestao_his_staging as gestao_his
    on his.ua_ug_cd_ug = gestao_his.ug_cd_ug
    and his.dt_historico = gestao_his.dt_historico
left join
    natureza_juridica_staging as nj on ua.natjur_cd_nat_juridica = nj.cd_nat_juridica
left join
    tipo_movimento_ua_staging as tm
    on his.tipmovua_cd_tipo_movimento = tm.cd_tipo_movimento
left join competencia_staging as comp on his.compet_cd_competencia = comp.cd_competencia
