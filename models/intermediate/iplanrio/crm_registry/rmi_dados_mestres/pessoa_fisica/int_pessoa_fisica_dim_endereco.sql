-- Consolidated enderecos de pessoa física a partir de múltiplas fontes do município
-- do Rio de Janeiro
-- Este modelo gera um array de endereços por CPF, unificando dados de saúde,
-- assistência social e BCadastro
{{
    config(
        alias="dim_endereco",
        schema="intermediario_dados_mestres",
        materialized=("table" if target.name == "dev" else "ephemeral"),
    )
}}

with

    -- SOURCE
    source_cep as (select * from {{ source("br_bd_diretorios_brasil", "cep") }}),

    all_prefeitura as (
         select cpf, cpf_particao from {{ ref("int_pessoa_fisica_all_cpf") }}
    ),

    source_bcadastro as (
        select b.*
        from all_prefeitura a
        inner join {{ source("brutos_bcadastro", "cpf") }} b using (cpf_particao)
    ),

    source_sms as (
        select b.*
        from all_prefeitura a
        inner join {{ source("rj-sms", "paciente") }} b using (cpf_particao)
    ),

    source_cadunico as (
        select b.*
        from all_prefeitura a
        inner join {{ source("rj-smas", "cadastros") }} b using (cpf_particao)
    ),


    source_georreferenciamento as (
        select
            DISTINCT
            logradouro_tratado,
            numero_porta,
            bairro,
            latitude,
            longitude,
            pluscode
        from {{ source("rj-crm-registy-intermediario-dados-mestres-staging", "enderecos_geolocalizados") }}
    ),

    -- ENDEREÇOS
    bcadastro as (
        select
            cpf,
            struct(
                endereco.municipio,
                endereco.uf,
                endereco.cep,
                endereco.bairro,
                endereco.tipo_logradouro,
                endereco.logradouro,
                endereco.complemento,
                endereco.numero,
                residente_exterior_indicador
            ) as dados
        from source_bcadastro
    ),

    sms as (select cpf, endereco from source_sms),

    cadunico as (select cpf, endereco from source_cadunico),

    endereco_geral as (
        select
            cpf,
            endereco.estado as estado,
            endereco.cidade as municipio,
            endereco.cep as cep,
            endereco.tipo_logradouro as tipo_logradouro,
            endereco.logradouro as logradouro,
            endereco.numero as numero,
            endereco.complemento as complemento,
            endereco.bairro as bairro,
            endereco.sistema as sistema,
            endereco.rank as rank,
            'sms' as source
        from sms, unnest(endereco) endereco
        union all
        select
            cpf,
            endereco.sigla_uf as estado,
            endereco.nome_municipio as municipio,
            endereco.cep as cep,
            endereco.tipo_logradouro as tipo_logradouro,
            endereco.logradouro as logradouro,
            cast(endereco.numero_logradouro as string) as numero,
            endereco.complemento as complemento,
            endereco.localidade as bairro,
            'cadunico' as sistema,
            1 as rank,
            'smas' as source
        from cadunico, unnest(endereco) endereco
        union all
        select
            cpf,
            dados.uf as estado,
            dados.municipio,
            dados.cep,
            dados.tipo_logradouro,
            dados.logradouro,
            dados.numero,
            dados.complemento,
            dados.bairro,
            'bacadastro' as sistema,
            1 as rank,
            'receita_federal' as source
        from bcadastro
    ),

    -- endereços corrigidos
    endereco_corrigido as (
        select
            cpf,
            cep.cep,
            coalesce(cep.sigla_uf, endereco.estado) as estado,
            coalesce(cep.nome_municipio, endereco.municipio) as municipio,
            endereco.tipo_logradouro,
            endereco.logradouro,
            endereco.numero,
            endereco.complemento,
            coalesce(cep.localidade, endereco.bairro) as bairro,
            endereco.sistema,
            endereco.rank,
            endereco.source
        from all_prefeitura
        left join endereco_geral as endereco using (cpf)
        left join source_cep as cep on endereco.cep = cep.cep
    ),

    -- tratar endereço para georreferenciamento
    tratar_endereco_corrigido as (
        select
            *,
            LOWER(REGEXP_REPLACE({{ proper_br("logradouro") }}, r'[^a-zA-Z0-9 ]', '')) AS logradouro_geo,
            LOWER(REGEXP_REPLACE(numero, r'[^a-zA-Z0-9 ]', '')) AS numero_porta_geo,
            LOWER(REGEXP_REPLACE({{ proper_br("bairro") }}, r'[^a-zA-Z0-9 ]', '')) AS bairro_geo
        from endereco_corrigido

    ),

    -- integrar com georreferenciamento
    geo_endereco_corrigido as (
        select
            endereco.cpf,
            endereco.cep,
            endereco.estado,
            endereco.municipio,
            endereco.tipo_logradouro,
            endereco.logradouro,
            endereco.numero,
            endereco.complemento,
            endereco.bairro,
            endereco.sistema,
            endereco.rank,
            endereco.source,
            geo.latitude,
            geo.longitude,
            geo.pluscode
        from tratar_endereco_corrigido endereco
        left join source_georreferenciamento geo
            on endereco.logradouro_geo = geo.logradouro_tratado
            and endereco.numero_porta_geo = geo.numero_porta
            and endereco.bairro_geo = geo.bairro

    ),

    -- Ordena os endereços por rank
    endereco_ranqueado as (
        select
            *,
            case
                when source = 'sms'
                then 1
                when source = 'smas'
                then 2
                when source = 'receita_federal'
                then 3
                else 4
            end as rank_source
        from geo_endereco_corrigido
    ),

    endereco_ordernado_agrupado as (
        select
            cpf,
            array_agg(
                struct(
                    source as origem,
                    lower(sistema) as sistema,
                    cep,
                    lower(estado) as estado,
                    {{ proper_br("municipio") }} as municipio,
                    lower(tipo_logradouro) as tipo_logradouro,
                    {{ proper_br("logradouro") }} as logradouro,
                    numero,
                    {{ proper_br("complemento") }} as complemento,
                    {{ proper_br("bairro") }} as bairro,
                    latitude,
                    longitude,
                    pluscode
                )
                order by rank_source asc, rank asc
            ) as endereco
        from endereco_ranqueado
        group by cpf
    ),

    endereco_principal_alternativo as (
        select
            cpf,
            endereco[offset(0)] as principal,
            array(
                select as struct * except (pos)
                from unnest(endereco)
                with offset pos
                where pos > 0
            ) as alternativo
        from endereco_ordernado_agrupado
    ),

    dim_endereco as (
        select
            cpf,
            struct(
                if(principal is not null, true, false) as indicador,
                principal,
                alternativo
            ) as endereco
        from all_prefeitura
        left join endereco_principal_alternativo using (cpf)
    )

select *
from dim_endereco
