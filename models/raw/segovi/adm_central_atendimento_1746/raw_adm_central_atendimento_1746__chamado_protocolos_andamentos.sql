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
        array_agg(distinct protocolo.ds_codigo_fk ignore nulls order by protocolo.ds_codigo_fk) as protocolos
    from {{ source('brutos_1746_staging_airbyte', 'tb_protocolo_chamado') }} as protocolo_chamado
    inner join {{ source('brutos_1746_staging_airbyte', 'vw_protocolo') }} as protocolo
        on protocolo.id_protocolo = protocolo_chamado.id_protocolo_fk
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
)

select
    chamado.id_chamado,
    ifnull(protocolos.protocolos, []) as protocolos,
    bairro.no_bairro as bairro,
    logradouro.no_logradouro as logradouro,
    chamado.ds_endereco_numero as numero,
    nullif(trim(chamado.ds_endereco_complemento), '') as complemento,
    nullif(trim(chamado.ds_endereco_referencia), '') as referencia,
    nullif(trim(chamado.ds_endereco_cep), '') as cep,
    ifnull(andamentos.andamentos, []) as andamentos,
    current_timestamp() as loaded_at,
    SAFE_CAST(DATE_TRUNC(DATE(chamado.dt_inicio), day) AS DATE) data_particao
from `rj-iplanrio.brutos_1746_staging.tb_chamado` as chamado
left join `rj-iplanrio.brutos_1746_staging.tb_bairro_logradouro` as bairro_logradouro
    on bairro_logradouro.id_bairro_logradouro = chamado.id_bairro_logradouro_fk
left join `rj-iplanrio.brutos_1746_staging.tb_bairro` as bairro
    on bairro.id_bairro = bairro_logradouro.id_bairro_fk
left join `rj-iplanrio.brutos_1746_staging.tb_logradouro` as logradouro
    on logradouro.id_logradouro = bairro_logradouro.id_logradouro_fk
left join protocolos
    on protocolos.id_chamado = chamado.id_chamado
left join andamentos
    on andamentos.id_chamado = chamado.id_chamado
