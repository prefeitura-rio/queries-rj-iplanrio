{{
    config(
        alias="nulls_cno",
        schema='debug_bcadastro',
        materialized=('table' if target.name == 'dev' else 'ephemeral'),
    )
}}


with
    -- Step 1: Get the total number of rows efficiently
    total_row_count as (
        select count(*) as total_rows
        from `rj-iplanrio.brutos_bcadastro_staging.chcno_bcadastros`  -- Target table for CNO
    ),
    -- Step 2: Parse the JSON and extract fields without window functions or
    -- aggressive casting/nullif
    -- Extract all listed keys using JSON_VALUE or appropriate functions (PARSE_DATE,
    -- EXTRACT_ARRAY)
    fonte_extracted as (
        select

            -- Extract keys from the provided list
            json_value(doc, '$.dataInicioObra') as datainicioobra,  -- Likely date string
            json_value(doc, '$.cadastroImobiliario') as cadastroimobiliario,  -- String
            json_value(doc, '$.cib') as cib,  -- String
            json_value(
                doc, '$.dataInicioResponsabilidade'
            ) as datainicioresponsabilidade,  -- Likely date string
            json_value(doc, '$.rrt') as rrt,  -- String
            json_value(doc, '$.logradouro') as logradouro,  -- String
            json_value(doc, '$.cep') as cep,  -- String
            json_value(doc, '$.tipoLogradouro') as tipologradouro,  -- String
            json_value(doc, '$.valorMedida') as valormedida,  -- Likely numeric string
            json_value(doc, '$.timestamp') as timestamp,  -- String (often epoch time)
            json_value(doc, '$.cno') as cno,  -- String (Primary key from doc)
            json_value(doc, '$.complemento') as complemento,  -- String
            json_value(doc, '$.situacao') as situacao,  -- String
            json_value(doc, '$.art') as art,  -- String
            json_value(doc, '$.municipio') as municipio,  -- String (Municipality name)
            json_value(doc, '$.numeroLogradouro') as numerologradouro,  -- String
            json_value(doc, '$.bairro') as bairro,  -- String
            json_value(doc, '$.codigoMunicipio') as codigomunicipio,  -- Likely municipality code string
            json_value(doc, '$.cnoVinculado') as cnovinculado,  -- String
            json_value(doc, '$.dataSituacao') as datasituacao,  -- Likely date string
            json_value(doc, '$.niResponsavel') as niresponsavel,  -- String
            json_value(doc, '$.tipo') as tipo,  -- String
            json_value(doc, '$.tipoResponsabilidade') as tiporesponsabilidade,  -- String
            json_value(doc, '$.uf') as uf,  -- String
            json_value(doc, '$.unidadeMedida') as unidademedida,  -- String

            -- Keys requiring special handling based on your CPNJ/CPF models or name
            -- '~version' requires REPLACE
            json_value(replace(to_json_string(doc), '~', ''), '$.version') as version,  -- String

            -- Technical fields from doc, based on your CPF model
            json_value(doc, '$._id') as _id,  -- String
            json_value(doc, '$._rev') as _rev,  -- String

            -- 'areas' looks like an array
            json_extract_array(doc, '$.areas') as areas  -- Array or NULL

        from `rj-iplanrio.brutos_bcadastro_staging.chcno_bcadastros`
    ),

    -- Step 3: Calculate counts for each column using UNION ALL
    column_counts as (
        -- For each column in fonte_extracted, create a SELECT statement
        -- Handle STRING, DATE, ARRAY, and other scalar types differently for empty
        -- counts
        -- --- STRING Columns (JSON_VALUE extractions unless noted) ---
        select
            'dataInicioObra' as column_name,
            sum(case when datainicioobra is null then 1 else 0 end) as null_count,
            sum(case when datainicioobra = '' then 1 else 0 end) as empty_count,
            sum(
                case
                    when datainicioobra is not null and datainicioobra != ''
                    then 1
                    else 0
                end
            ) as other_count
        from fonte_extracted
        union all
        select
            'cadastroImobiliario' as column_name,
            sum(case when cadastroimobiliario is null then 1 else 0 end),
            sum(case when cadastroimobiliario = '' then 1 else 0 end),
            sum(
                case
                    when cadastroimobiliario is not null and cadastroimobiliario != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'cib' as column_name,
            sum(case when cib is null then 1 else 0 end),
            sum(case when cib = '' then 1 else 0 end),
            sum(case when cib is not null and cib != '' then 1 else 0 end)
        from fonte_extracted
        union all
        select
            'dataInicioResponsabilidade' as column_name,
            sum(case when datainicioresponsabilidade is null then 1 else 0 end),
            sum(case when datainicioresponsabilidade = '' then 1 else 0 end),
            sum(
                case
                    when
                        datainicioresponsabilidade is not null
                        and datainicioresponsabilidade != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'rrt' as column_name,
            sum(case when rrt is null then 1 else 0 end),
            sum(case when rrt = '' then 1 else 0 end),
            sum(case when rrt is not null and rrt != '' then 1 else 0 end)
        from fonte_extracted
        union all
        select
            'logradouro' as column_name,
            sum(case when logradouro is null then 1 else 0 end),
            sum(case when logradouro = '' then 1 else 0 end),
            sum(case when logradouro is not null and logradouro != '' then 1 else 0 end)
        from fonte_extracted
        union all
        select
            'cep' as column_name,
            sum(case when cep is null then 1 else 0 end),
            sum(case when cep = '' then 1 else 0 end),
            sum(case when cep is not null and cep != '' then 1 else 0 end)
        from fonte_extracted
        union all
        select
            'tipoLogradouro' as column_name,
            sum(case when tipologradouro is null then 1 else 0 end),
            sum(case when tipologradouro = '' then 1 else 0 end),
            sum(
                case
                    when tipologradouro is not null and tipologradouro != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'valorMedida' as column_name,
            sum(case when valormedida is null then 1 else 0 end),
            sum(case when valormedida = '' then 1 else 0 end),
            sum(
                case
                    when valormedida is not null and valormedida != '' then 1 else 0
                end
            )
        from fonte_extracted
        union all
        select
            'timestamp' as column_name,
            sum(case when timestamp is null then 1 else 0 end),
            sum(case when timestamp = '' then 1 else 0 end),
            sum(case when timestamp is not null and timestamp != '' then 1 else 0 end)
        from fonte_extracted
        union all
        select
            'cno' as column_name,
            sum(case when cno is null then 1 else 0 end),
            sum(case when cno = '' then 1 else 0 end),
            sum(case when cno is not null and cno != '' then 1 else 0 end)
        from fonte_extracted
        union all
        select
            'complemento' as column_name,
            sum(case when complemento is null then 1 else 0 end),
            sum(case when complemento = '' then 1 else 0 end),
            sum(
                case
                    when complemento is not null and complemento != '' then 1 else 0
                end
            )
        from fonte_extracted
        union all
        select
            'situacao' as column_name,
            sum(case when situacao is null then 1 else 0 end),
            sum(case when situacao = '' then 1 else 0 end),
            sum(case when situacao is not null and situacao != '' then 1 else 0 end)
        from fonte_extracted
        union all
        select
            'art' as column_name,
            sum(case when art is null then 1 else 0 end),
            sum(case when art = '' then 1 else 0 end),
            sum(case when art is not null and art != '' then 1 else 0 end)
        from fonte_extracted
        union all
        select
            'municipio' as column_name,
            sum(case when municipio is null then 1 else 0 end),
            sum(case when municipio = '' then 1 else 0 end),
            sum(case when municipio is not null and municipio != '' then 1 else 0 end)
        from fonte_extracted
        union all
        select
            'numeroLogradouro' as column_name,
            sum(case when numerologradouro is null then 1 else 0 end),
            sum(case when numerologradouro = '' then 1 else 0 end),
            sum(
                case
                    when numerologradouro is not null and numerologradouro != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'bairro' as column_name,
            sum(case when bairro is null then 1 else 0 end),
            sum(case when bairro = '' then 1 else 0 end),
            sum(case when bairro is not null and bairro != '' then 1 else 0 end)
        from fonte_extracted
        union all
        select
            'codigoMunicipio' as column_name,
            sum(case when codigomunicipio is null then 1 else 0 end),
            sum(case when codigomunicipio = '' then 1 else 0 end),
            sum(
                case
                    when codigomunicipio is not null and codigomunicipio != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'cnoVinculado' as column_name,
            sum(case when cnovinculado is null then 1 else 0 end),
            sum(case when cnovinculado = '' then 1 else 0 end),
            sum(
                case
                    when cnovinculado is not null and cnovinculado != '' then 1 else 0
                end
            )
        from fonte_extracted
        union all
        select
            'dataSituacao' as column_name,
            sum(case when datasituacao is null then 1 else 0 end),
            sum(case when datasituacao = '' then 1 else 0 end),
            sum(
                case
                    when datasituacao is not null and datasituacao != '' then 1 else 0
                end
            )
        from fonte_extracted
        union all
        select
            'niResponsavel' as column_name,
            sum(case when niresponsavel is null then 1 else 0 end),
            sum(case when niresponsavel = '' then 1 else 0 end),
            sum(
                case
                    when niresponsavel is not null and niresponsavel != '' then 1 else 0
                end
            )
        from fonte_extracted
        union all
        select
            'tipo' as column_name,
            sum(case when tipo is null then 1 else 0 end),
            sum(case when tipo = '' then 1 else 0 end),
            sum(case when tipo is not null and tipo != '' then 1 else 0 end)
        from fonte_extracted
        union all
        select
            'tipoResponsabilidade' as column_name,
            sum(case when tiporesponsabilidade is null then 1 else 0 end),
            sum(case when tiporesponsabilidade = '' then 1 else 0 end),
            sum(
                case
                    when tiporesponsabilidade is not null and tiporesponsabilidade != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'uf' as column_name,
            sum(case when uf is null then 1 else 0 end),
            sum(case when uf = '' then 1 else 0 end),
            sum(case when uf is not null and uf != '' then 1 else 0 end)
        from fonte_extracted
        union all
        select
            'unidadeMedida' as column_name,
            sum(case when unidademedida is null then 1 else 0 end),
            sum(case when unidademedida = '' then 1 else 0 end),
            sum(
                case
                    when unidademedida is not null and unidademedida != '' then 1 else 0
                end
            )
        from fonte_extracted
        union all
        select
            'version' as column_name,
            sum(case when version is null then 1 else 0 end),
            sum(case when version = '' then 1 else 0 end),
            sum(case when version is not null and version != '' then 1 else 0 end)
        from fonte_extracted
        union all
        select
            '_id' as column_name,
            sum(case when _id is null then 1 else 0 end),
            sum(case when _id = '' then 1 else 0 end),
            sum(case when _id is not null and _id != '' then 1 else 0 end)
        from fonte_extracted
        union all
        select
            '_rev' as column_name,
            sum(case when _rev is null then 1 else 0 end),
            sum(case when _rev = '' then 1 else 0 end),
            sum(case when _rev is not null and _rev != '' then 1 else 0 end)
        from fonte_extracted
        union all

        -- --- ARRAY Columns ---
        select
            'areas' as column_name,
            sum(case when areas is null then 1 else 0 end) as null_count,
            sum(case when array_length(areas) = 0 then 1 else 0 end) as empty_count,
            sum(
                case
                    when areas is not null and array_length(areas) > 0 then 1 else 0
                end
            ) as other_count
        from fonte_extracted

    -- Add a UNION ALL block for every column extracted in fonte_extracted
    )

-- Step 4: Calculate percentages by joining counts with total count
select
    cc.column_name as key,
    round(safe_divide(cc.null_count * 100, trc.total_rows), 2) as null_percent,
    round(safe_divide(cc.empty_count * 100, trc.total_rows), 2) as empty_percent,
    round(safe_divide(cc.other_count * 100, trc.total_rows), 2) as others_percent  -- Non-null, non-empty
from column_counts as cc
cross join  -- Joins each row from column_counts with the single row from total_row_count
    total_row_count as trc
order by null_percent desc  -- Order by null percentage descending
