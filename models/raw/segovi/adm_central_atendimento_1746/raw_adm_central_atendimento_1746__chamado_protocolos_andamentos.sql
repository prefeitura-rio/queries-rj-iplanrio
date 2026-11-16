{{
    config(
        alias='chamado_protocolos_andamentos',
        schema='adm_central_atendimento_1746',
        materialized='table',
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        }
    )
}}


with protocolos as (
    select
        protocolo_chamado.id_chamado_fk as id_chamado,
        array_agg(
            struct(protocolo.ds_codigo_fk as codigo_protocolo,
                    case
                        when ifnull(pessoa.ds_cpf,'') = '' then null
                        else lpad(pessoa.ds_cpf, 11, '0')
                    end as cpf) order by protocolo.ds_codigo_fk) as protocolos
    from {{ source('brutos_1746_staging_airbyte', 'tb_protocolo_chamado') }} as protocolo_chamado
    inner join {{ source('brutos_1746_staging_airbyte', 'vw_protocolo') }} as protocolo
        on protocolo.id_protocolo = protocolo_chamado.id_protocolo_fk
    left join {{ source('brutos_1746_staging_airbyte', 'vw_pessoa') }} as pessoa
        on pessoa.id_pessoa = protocolo.id_pessoa_fk
    group by protocolo_chamado.id_chamado_fk
),

andamentos as (
    select
        andamento.id_chamado_fk as id_chamado,
        array_agg(struct(
            andamento.id_andamento,
            andamento.dt_insercao as data,
            andamento.ds_andamento as descricao,
            unidade.no_unidade_organizacional as nome_unidade_organizacional,
            andamento.fl_publico
        ) order by andamento.dt_insercao) as andamentos
    from {{ source('brutos_1746_staging_airbyte', 'tb_andamento') }} as andamento
    left join {{ source('brutos_1746_staging_airbyte', 'tb_unidade_organizacional') }} as unidade
        on unidade.id_unidade_organizacional = andamento.id_unidade_organizacional_fk
    where andamento.fl_publico = true
    group by andamento.id_chamado_fk
),

ultima_classificacao_chamado as (
    select a.id_chamado_fk, b.id_subtipo, b.no_subtipo as subtipo, c.id_tipo, c.no_tipo as tipo
    from {{ source('brutos_1746_staging_airbyte', 'tb_classificacao_chamado') }} as a
    left join {{ source('brutos_1746_staging_airbyte', 'tb_subtipo') }} as b 
        on b.id_subtipo = a.id_subtipo_fk
    left join {{ source('brutos_1746_staging_airbyte', 'tb_tipo') }} as c
        on c.id_tipo = b.id_tipo_fk
    qualify row_number() over(partition by id_chamado_fk order by dt_classificacao desc) = 1
),

responsavel_atual as (
    select andamento.id_chamado_fk, unidade.no_unidade_organizacional as unidade_organizacional_responsavel_atual
    from {{ source('brutos_1746_staging_airbyte', 'tb_andamento') }} as andamento
    left join {{ source('brutos_1746_staging_airbyte', 'tb_unidade_organizacional') }} as unidade
        on unidade.id_unidade_organizacional = andamento.id_unidade_organizacional_fk
    qualify row_number() over(partition by andamento.id_chamado_fk order by andamento.dt_insercao desc) = 1
)

select
    chamado.id_chamado,
    chamado.ds_chamado as descricao_chamado,
    chamado.dt_inicio as data_inicio,
    chamado.dt_fim as data_fim,
    sla.dt_alvo_finalizacao as data_prevista_atendimento,
    protocolos.protocolos as protocolos,
    bairro.no_bairro as bairro,
    logradouro.no_logradouro as logradouro,
    chamado.ds_endereco_numero as numero,
    nullif(trim(chamado.ds_endereco_complemento), '') as complemento,
    nullif(trim(chamado.ds_endereco_referencia), '') as referencia,
    nullif(trim(chamado.ds_endereco_cep), '') as cep,
    status.no_status as status, 
    origem.no_origem_ocorrencia as origem_ocorrencia,
    categoria.no_categoria as categoria,
    classificacao.tipo,
    classificacao.subtipo,
    responsavel.unidade_organizacional_responsavel_atual,
    ifnull(andamentos.andamentos, []) as andamentos,
    current_timestamp() as loaded_at,
    SAFE_CAST(DATE_TRUNC(DATE(chamado.dt_inicio), day) AS DATE) data_particao
from {{ source('brutos_1746_staging_airbyte', 'tb_chamado') }} as chamado
left join {{ source('brutos_1746_staging_airbyte', 'tb_bairro_logradouro') }} as bairro_logradouro
    on bairro_logradouro.id_bairro_logradouro = chamado.id_bairro_logradouro_fk
left join {{ source('brutos_1746_staging_airbyte', 'tb_bairro') }} as bairro
    on bairro.id_bairro = bairro_logradouro.id_bairro_fk
left join {{ source('brutos_1746_staging_airbyte', 'tb_logradouro') }} as logradouro
    on logradouro.id_logradouro = bairro_logradouro.id_logradouro_fk
left join {{ source('brutos_1746_staging_airbyte', 'vw_pessoa') }} as pessoa
    on pessoa.id_pessoa = chamado.id_pessoa_fk
left join {{ source('brutos_1746_staging_airbyte', 'tb_status') }} as status
    on status.id_status = chamado.id_status_fk
left join {{ source('brutos_1746_staging_airbyte', 'tb_origem_ocorrencia') }} as origem
    on origem.id_origem_ocorrencia = chamado.id_origem_ocorrencia_fk
left join {{ source('brutos_1746_staging_airbyte', 'tb_chamado_sla') }} as sla
    on sla.id_chamado_fk = chamado.id_chamado
left join {{ source('brutos_1746_staging_airbyte', 'tb_categoria') }} as categoria
    on categoria.id_categoria = chamado.id_categoria_fk
left join ultima_classificacao_chamado as classificacao
    on classificacao.id_chamado_fk = chamado.id_chamado
left join responsavel_atual as responsavel
    on responsavel.id_chamado_fk = chamado.id_chamado
left join protocolos
    on protocolos.id_chamado = chamado.id_chamado
left join andamentos
    on andamentos.id_chamado = chamado.id_chamado
