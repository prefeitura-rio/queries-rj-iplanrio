/*
plus11

id_equipamento STRING
secretaria_responsavel STRING
tipo_equipamento STRING
nome_oficial STRING
nome_popular STRING

plus10 STRING
plus11 STRING
plus6 STRING
latitude FLOAT64
longitude FLOAT64
geometry GEOGRAPHY

endereco STRUCT<
    logradouro  STRING,
    numero      STRING,
    complemento STRING,
    bairro      STRING,
    cep         STRING
>




bairro STRUCT<
    id_bairro STRING,
    bairro STRING,
    nome_regiao_planejamento STRING,
    nome_regiao_administrativa STRING,
    subprefeitura STRING,


>
contato STRUCT<
    telefones   ARRAY<STRING>,
    email       STRING,
    site        STRING,
    redes_social STRUCT<
    facebook  STRING,
    instagram STRING,
    twitter   STRING
    >
>

ativo                    BOOL,
aberto_ao_publico        BOOL,
horario_funcionamento        ARRAY<STRUCT<dia STRING, abre TIME, fecha TIME>>


fonte                    STRING,
vigencia_inicio          DATE,
vigencia_fim             DATE

metadata                 JSON,

updated_at      TIMESTAMP,
*/
-- do union all with equipaments from other sources
{# CREATE OR REPLACE TABLE `rj-iplanrio.plus_codes.equipamentos` AS ( #}
{{
    config(
        alias="equipamentos",
        schema="plus_codes",
        materialized="table",
    )
}}

with
    controle_especifico as (
        select
            secretaria_responsavel,
            trim(tipo_equipamento) as tipo_equipamento,
            trim(tipo) as tipo,
            nome,
            case when use = '0' then false else true end as use
        from {{ source("plus_codes", "equipamentos_controle_categorias") }}
    ),

    controle_generico as (
        select
            secretaria_responsavel,
            trim(tipo_equipamento) as tipo_equipamento,
            trim(tipo) as tipo,
            use
        from controle_especifico
    ),

    saude as (
        select
            plus8,
            geometry,
            plus11,
            id_equipamento,
            secretaria_responsavel,
            tipo_equipamento,
            nome_oficial,
            nome_popular,
            plus10,
            plus6,
            latitude,
            longitude,
            endereco,
            bairro,
            contato,
            ativo,
            aberto_ao_publico,
            horario_funcionamento,
            fonte,
            vigencia_inicio,
            vigencia_fim,
            esfera,
            metadata,
            updated_at
        from {{ ref("raw_equipamentos_saude") }}
    ),

    saude_territorio_unidades as (
        select
            plus8,
            geometry,
            plus11,
            id_equipamento,
            secretaria_responsavel,
            tipo_equipamento,
            nome_oficial,
            nome_popular,
            plus10,
            plus6,
            latitude,
            longitude,
            endereco,
            bairro,
            contato,
            ativo,
            aberto_ao_publico,
            horario_funcionamento,
            fonte,
            vigencia_inicio,
            vigencia_fim,
            esfera,
            metadata,
            updated_at
        from {{ ref("raw_equipamentos_saude_unidades") }}
    ),


    saude_territorio_equipes as (
        select
            plus8,
            geometry,
            plus11,
            id_equipamento,
            secretaria_responsavel,
            tipo_equipamento,
            nome_oficial,
            nome_popular,
            plus10,
            plus6,
            latitude,
            longitude,
            endereco,
            bairro,
            contato,
            ativo,
            aberto_ao_publico,
            horario_funcionamento,
            fonte,
            vigencia_inicio,
            vigencia_fim,
            esfera,
            metadata,
            updated_at
        from {{ ref("raw_equipamentos_saude_equipe_familia") }}
    ),

    educacao as (
        select
            plus8,
            geometry,
            plus11,
            id_equipamento,
            secretaria_responsavel,
            tipo_equipamento,
            nome_oficial,
            nome_popular,
            plus10,
            plus6,
            latitude,
            longitude,
            endereco,
            bairro,
            contato,
            ativo,
            aberto_ao_publico,
            horario_funcionamento,
            fonte,
            vigencia_inicio,
            vigencia_fim,
            CAST(NULL AS STRING) AS esfera,
            metadata,
            updated_at
        from {{ ref("raw_equipamentos_educacao") }}
    ),

    cultura as (
        select
            plus8,
            geometry,
            plus11,
            id_equipamento,
            secretaria_responsavel,
            tipo_equipamento,
            nome_oficial,
            nome_popular,
            plus10,
            plus6,
            latitude,
            longitude,
            endereco,
            bairro,
            contato,
            ativo,
            aberto_ao_publico,
            horario_funcionamento,
            fonte,
            vigencia_inicio,
            vigencia_fim,
            CAST(NULL AS STRING) AS esfera,
            metadata,
            updated_at
        from {{ ref("raw_equipamentos_cultura") }}
    ),

    assistencia_social as (
        select
            plus8,
            geometry,
            plus11,
            id_equipamento,
            secretaria_responsavel,
            tipo_equipamento,
            nome_oficial,
            nome_popular,
            plus10,
            plus6,
            latitude,
            longitude,
            endereco,
            bairro,
            contato,
            ativo,
            aberto_ao_publico,
            horario_funcionamento,
            fonte,
            vigencia_inicio,
            vigencia_fim,
            CAST(NULL AS STRING) AS esfera,
            metadata,
            updated_at
        from {{ ref("raw_equipamentos_assistencia_social") }}
    ),

    pontos_apoio as (
        select
            plus8,
            geometry,
            plus11,
            id_equipamento,
            secretaria_responsavel,
            tipo_equipamento,
            nome_oficial,
            nome_popular,
            plus10,
            plus6,
            latitude,
            longitude,
            endereco,
            bairro,
            contato,
            ativo,
            aberto_ao_publico,
            horario_funcionamento,
            fonte,
            vigencia_inicio,
            vigencia_fim,
            CAST(NULL AS STRING) AS esfera,
            metadata,
            updated_at
        from {{ ref("raw_equipamentos_pontos_apoio") }}
    ),

    equipamentos as (
        select *
        from saude
        union all
        select *
        from saude_territorio_unidades
        union all
        select *
        from saude_territorio_equipes
        union all
        select *
        from educacao
        union all
        select *
        from cultura
        union all
        select *
        from assistencia_social
        union all
        select *
        from pontos_apoio
    ),

    equipamentos_categorias as (
        select distinct
            eq.plus8,
            eq.plus11,
            eq.id_equipamento,
            eq.secretaria_responsavel,
            coalesce(c_esp.tipo, c_gen.tipo, eq.tipo_equipamento) as categoria,
            coalesce(c_esp.use, c_gen.use, true) as use,
            eq.tipo_equipamento,
            eq.nome_oficial,
            eq.nome_popular,
            eq.plus10,
            eq.plus6,
            eq.latitude,
            eq.longitude,
            eq.endereco,
            eq.bairro,
            eq.contato,
            eq.ativo,
            eq.aberto_ao_publico,
            eq.horario_funcionamento,
            eq.fonte,
            eq.vigencia_inicio,
            eq.vigencia_fim,
            eq.esfera,
            eq.metadata,
            eq.updated_at,
            ST_ASTEXT(eq.geometry) as geometry_text
        from equipamentos eq
        -- 1. Tenta fazer o JOIN com as regras específicas primeiro
        left join
            controle_especifico as c_esp
            on eq.secretaria_responsavel = c_esp.secretaria_responsavel
            and eq.tipo_equipamento = c_esp.tipo_equipamento
            and eq.nome_oficial like '%' || c_esp.nome || '%'
        -- 2. Tenta fazer o JOIN com as regras genéricas em paralelo
        left join
            controle_generico as c_gen
            on eq.secretaria_responsavel = c_gen.secretaria_responsavel
            and eq.tipo_equipamento = c_gen.tipo_equipamento
        order by eq.secretaria_responsavel, eq.tipo_equipamento
    )

select
    eq.plus8,
    eq.plus11,
    eq.id_equipamento,
    ST_GEOGFROMTEXT(eq.geometry_text) as geometry,
    eq.secretaria_responsavel,
    eq.categoria,
    eq.use,
    eq.tipo_equipamento,
    eq.nome_oficial,
    eq.nome_popular,
    eq.plus10,
    eq.plus6,
    eq.latitude,
    eq.longitude,
    eq.endereco,
    eq.bairro,
    eq.contato,
    eq.ativo,
    eq.aberto_ao_publico,
    eq.horario_funcionamento,
    eq.fonte,
    eq.vigencia_inicio,
    eq.vigencia_fim,
    eq.esfera,
    eq.metadata,
    eq.updated_at
from equipamentos_categorias eq

