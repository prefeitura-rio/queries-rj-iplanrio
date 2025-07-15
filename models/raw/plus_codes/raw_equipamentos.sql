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
            metadata,
            updated_at
        from `rj-iplanrio`.`plus_codes`.`equipamentos_saude`
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
            metadata,
            updated_at
        from `rj-iplanrio`.`plus_codes`.`equipamentos_educacao`
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
            metadata,
            updated_at
        from `rj-iplanrio`.`plus_codes`.`equipamentos_cultura`
    ),

    equipamentos as (
        select *
        from saude
        union all
        select *
        from educacao
        union all
        select *
        from cultura
    ),

    equipamentos_categorias as (
        select
            eq.plus8,
            eq.geometry,
            eq.plus11,
            eq.id_equipamento,
            eq.secretaria_responsavel,
            eq.tipo_equipamento as categoria,
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
            eq.metadata,
            eq.updated_at
        from equipamentos eq
    )

select *
from equipamentos_categorias
