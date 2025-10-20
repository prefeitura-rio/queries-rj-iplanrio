-- tabela de CNO - Cadastro Nacional de Obras
{{
    config(
        alias="chcno_bcadastros_parsed",
        schema="brutos_bcadastro_staging",
        materialized="table",
    )
}}


with
    fonte as (
        select *
        from {{ source("brutos_bcadastro_staging", "chcno_bcadastros") }}

        {% if target.name == "dev" %}
            where
                timestamp(_airbyte_extracted_at)
                >= timestamp_sub(current_timestamp(), interval 3 day)
        {% endif %}
    ),

    fonte_parseada as (
        select
            -- Campos extraídos do JSON conforme a imagem fornecida
            nullif(json_value(doc, '$.areas'), '') as areas,
            nullif(json_value(doc, '$.art'), '') as art,
            nullif(json_value(doc, '$.bairro'), '') as bairro,
            nullif(json_value(doc, '$.cadastromobiliario'), '') as cadastromobiliario,
            nullif(json_value(doc, '$.cep'), '') as cep,
            nullif(json_value(doc, '$.cib'), '') as cib,
            nullif(json_value(doc, '$.cno'), '') as cno,
            nullif(json_value(doc, '$.cnoVinculado'), '') as cnovinculado,
            nullif(json_value(doc, '$.codigoMunicipio'), '') as codigomunicipio,
            nullif(json_value(doc, '$.complemento'), '') as complemento,
            safe.parse_date(
                '%Y-%m-%d', nullif(json_value(doc, '$.dataInicioObra'), '')
            ) as datainicioobra,
            safe.parse_date(
                '%Y-%m-%d', nullif(json_value(doc, '$.dataInicioResponsabilidade'), '')
            ) as datainicioresponsabilidade,
            safe.parse_date(
                '%Y-%m-%d', nullif(json_value(doc, '$.dataSituacao'), '')
            ) as datasituacao,
            nullif(json_value(doc, '$.id_doc'), '') as id_doc,
            nullif(json_value(doc, '$.logradouro'), '') as logradouro,
            nullif(json_value(doc, '$.municipio'), '') as municipio,
            nullif(json_value(doc, '$.niResponsavel'), '') as niresponsavel,
            nullif(json_value(doc, '$.numeroLogradouro'), '') as numerologradouro,
            nullif(json_value(doc, '$._rev'), '') as _rev,
            nullif(json_value(doc, '$.rrt'), '') as rrt,
            nullif(json_value(doc, '$.situacao'), '') as situacao,
            nullif(json_value(doc, '$.timestamp'), '') as timestamp,
            nullif(json_value(doc, '$.tipo'), '') as tipo,
            nullif(json_value(doc, '$.tipoLogradouro'), '') as tipologradouro,
            nullif(
                json_value(doc, '$.tipoResponsabilidade'), ''
            ) as tiporesponsabilidade,
            nullif(json_value(doc, '$.uf'), '') as uf,
            nullif(json_value(doc, '$.unidadeMedida'), '') as unidademedida,
            nullif(json_value(doc, '$.valorMedida'), '') as valormedida,
            nullif(json_value(doc, '$.version'), '') as version,
            nullif(json_value(doc, '$._id'), '') as _id,

            -- Airbyte fields
            struct(
                _airbyte_raw_id as raw_id,
                _airbyte_extracted_at as extracted_at,
                _airbyte_generation_id as generation_id,
                nullif(json_value(_airbyte_meta, '$.changes'), "") as changes,
                nullif(json_value(_airbyte_meta, '$.sync_id'), "") as sync_id
            ) as airbyte,
        from fonte
    ),

    dedup as (
        select *,
        -- Partition by cno
            cast(cno as int64) as cno_particao,
        from fonte_parseada
        qualify
            row_number() over (partition by cno order by airbyte.extracted_at desc) = 1
    )

select *
from dedup
