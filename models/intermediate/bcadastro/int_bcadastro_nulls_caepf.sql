{{
    config(
        alias="nulls_caepf",
        schema='debug_bcadastro',
        materialized=('table' if target.name == 'dev' else 'ephemeral'),
    )
}}


with
    -- Step 1: Get the total number of rows efficiently
    total_row_count as (
        select count(*) as total_rows
        from `rj-iplanrio.brutos_bcadastro_staging.chcaepf_bcadastros`  -- Target table for CAEPF
    ),
    -- Step 2: Parse the JSON and extract fields without window functions or
    -- aggressive casting/nullif
    -- Extract all listed keys using JSON_VALUE or appropriate functions (PARSE_DATE,
    -- EXTRACT_ARRAY)
    fonte_extracted as (
        select

            -- Extract keys from the provided list using JSON_VALUE for scalar values
            -- Raw aliases added where conversion to DATE/other types might be
            -- intended later
            json_value(doc, '$.dataInicioAtividade') as datainicioatividade,  -- Likely date string
            json_value(doc, '$.nroCpf') as nrocpf,  -- String
            json_value(doc, '$.numeroLogradouro') as numerologradouro,  -- String
            json_value(doc, '$.dataSituacao') as datasituacao,  -- Likely date string
            json_value(doc, '$.nomeAtividade') as nomeatividade,  -- String
            json_value(doc, '$.nomeMotivoSituacao') as nomemotivosituacao,  -- String
            json_value(doc, '$.nomeMunicipio') as nomemunicipio,  -- String
            json_value(doc, '$.nomeSituacaoCpf') as nomesituacaocpf,  -- String
            json_value(doc, '$.telefoneContato') as telefonecontato,  -- String
            json_value(doc, '$.tipo') as tipo,  -- String
            json_value(doc, '$.cep') as cep,  -- String
            json_value(doc, '$.codSituacao') as codsituacao,  -- String
            json_value(doc, '$.nroAepfCompleto') as nroaepfcompleto,  -- String
            json_value(doc, '$.codMotivoSituacao') as codmotivosituacao,  -- String
            json_value(doc, '$.nomeTipoContribuinte') as nometipocontribuinte,  -- String
            json_value(doc, '$.bairro') as bairro,  -- String

            -- Special key '~version' requires REPLACE
            json_value(replace(to_json_string(doc), '~', ''), '$.version') as version,  -- String

            json_value(doc, '$.codOrgaoCpf') as codorgaocpf,  -- String
            json_value(doc, '$.complemento') as complemento,  -- String
            -- 'id' from source table is selected above
            json_value(doc, '$.codTipoContribuinte') as codtipocontribuinte,  -- String
            json_value(doc, '$.codAtividade') as codatividade,  -- String
            json_value(doc, '$.dataUltimaAtualizacao') as dataultimaatualizacao,  -- Likely date string
            json_value(doc, '$.nomeCpf') as nomecpf,  -- String
            json_value(doc, '$.nomeQualificacao') as nomequalificacao,  -- String
            json_value(doc, '$.language') as language,  -- String
            json_value(doc, '$.codOrgaoMunicipio') as codorgaomunicipio,  -- String
            json_value(doc, '$.codSituacaoCpf') as codsituacaocpf,  -- String
            json_value(doc, '$.ufMunicipio') as ufmunicipio,  -- String
            json_value(doc, '$.timestamp') as timestamp,  -- String (Duplicate key name in list, assuming it's the same $.timestamp)

            -- Technical fields from doc and source, based on your models
            json_value(doc, '$._id') as _id,  -- String from doc
            json_value(doc, '$._rev') as _rev,  -- String from doc

            -- 'cnaes' looks like an array - use JSON_EXTRACT_ARRAY
            json_extract_array(doc, '$.cnaes') as cnaes,  -- Array or NULL

            json_value(doc, '$.emailContato') as emailcontato,  -- String
            json_value(doc, '$.nomeSituacao') as nomesituacao,  -- String
            -- _rev already included
            json_value(doc, '$.cei') as cei,  -- String
            json_value(doc, '$.codMunicipio') as codmunicipio,  -- String
            json_value(doc, '$.logradouro') as logradouro,  -- String (Duplicate key name in list, assuming it's the same $.logradouro) - added suffix
            json_value(doc, '$.nroAepf') as nroaepf,  -- String
            json_value(doc, '$.views') as views,  -- JSON object/array -> analyzed as NULL by JSON_VALUE
            json_value(doc, '$.codQualificacao') as codqualificacao,  -- String
        -- Note: Removed '_airbyte_*' and 'value', 'seq', 'last_seq' as they weren't
        -- in the provided CNO key list
        -- If they exist in the source schema and need analysis, add them here.
        -- The provided key list is assumed to be the definitive set of JSON keys for
        -- this analysis.
        from `rj-iplanrio.brutos_bcadastro_staging.chcaepf_bcadastros`
    ),

    -- Step 3: Calculate counts for each column using UNION ALL
    column_counts as (
        -- For each column in fonte_extracted, create a SELECT statement
        -- Handle STRING, DATE, ARRAY, and other scalar types differently for empty
        -- counts
        -- --- STRING Columns (Most JSON_VALUE extractions) ---
        -- Includes raw string versions of potential dates/codes and nested view paths
        -- analyzed as scalar or NULL
        select
            'dataInicioAtividade' as column_name,
            sum(case when datainicioatividade is null then 1 else 0 end) as null_count,
            sum(case when datainicioatividade = '' then 1 else 0 end) as empty_count,
            sum(
                case
                    when datainicioatividade is not null and datainicioatividade != ''
                    then 1
                    else 0
                end
            ) as other_count
        from fonte_extracted
        union all
        select
            'nroCpf' as column_name,
            sum(case when nrocpf is null then 1 else 0 end),
            sum(case when nrocpf = '' then 1 else 0 end),
            sum(case when nrocpf is not null and nrocpf != '' then 1 else 0 end)
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
            'nomeAtividade' as column_name,
            sum(case when nomeatividade is null then 1 else 0 end),
            sum(case when nomeatividade = '' then 1 else 0 end),
            sum(
                case
                    when nomeatividade is not null and nomeatividade != '' then 1 else 0
                end
            )
        from fonte_extracted
        union all
        select
            'nomeMotivoSituacao' as column_name,
            sum(case when nomemotivosituacao is null then 1 else 0 end),
            sum(case when nomemotivosituacao = '' then 1 else 0 end),
            sum(
                case
                    when nomemotivosituacao is not null and nomemotivosituacao != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'nomeMunicipio' as column_name,
            sum(case when nomemunicipio is null then 1 else 0 end),
            sum(case when nomemunicipio = '' then 1 else 0 end),
            sum(
                case
                    when nomemunicipio is not null and nomemunicipio != '' then 1 else 0
                end
            )
        from fonte_extracted
        union all
        select
            'nomeSituacaoCpf' as column_name,
            sum(case when nomesituacaocpf is null then 1 else 0 end),
            sum(case when nomesituacaocpf = '' then 1 else 0 end),
            sum(
                case
                    when nomesituacaocpf is not null and nomesituacaocpf != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'telefoneContato' as column_name,
            sum(case when telefonecontato is null then 1 else 0 end),
            sum(case when telefonecontato = '' then 1 else 0 end),
            sum(
                case
                    when telefonecontato is not null and telefonecontato != ''
                    then 1
                    else 0
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
            'cep' as column_name,
            sum(case when cep is null then 1 else 0 end),
            sum(case when cep = '' then 1 else 0 end),
            sum(case when cep is not null and cep != '' then 1 else 0 end)
        from fonte_extracted
        union all
        select
            'codSituacao' as column_name,
            sum(case when codsituacao is null then 1 else 0 end),
            sum(case when codsituacao = '' then 1 else 0 end),
            sum(
                case
                    when codsituacao is not null and codsituacao != '' then 1 else 0
                end
            )
        from fonte_extracted
        union all
        select
            'nroAepfCompleto' as column_name,
            sum(case when nroaepfcompleto is null then 1 else 0 end),
            sum(case when nroaepfcompleto = '' then 1 else 0 end),
            sum(
                case
                    when nroaepfcompleto is not null and nroaepfcompleto != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'codMotivoSituacao' as column_name,
            sum(case when codmotivosituacao is null then 1 else 0 end),
            sum(case when codmotivosituacao = '' then 1 else 0 end),
            sum(
                case
                    when codmotivosituacao is not null and codmotivosituacao != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'nomeTipoContribuinte' as column_name,
            sum(case when nometipocontribuinte is null then 1 else 0 end),
            sum(case when nometipocontribuinte = '' then 1 else 0 end),
            sum(
                case
                    when nometipocontribuinte is not null and nometipocontribuinte != ''
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
            'version' as column_name,
            sum(case when version is null then 1 else 0 end),
            sum(case when version = '' then 1 else 0 end),
            sum(case when version is not null and version != '' then 1 else 0 end)
        from fonte_extracted
        union all
        select
            'codOrgaoCpf' as column_name,
            sum(case when codorgaocpf is null then 1 else 0 end),
            sum(case when codorgaocpf = '' then 1 else 0 end),
            sum(
                case
                    when codorgaocpf is not null and codorgaocpf != '' then 1 else 0
                end
            )
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
        -- id (analyzed below as source column)
        select
            'codTipoContribuinte' as column_name,
            sum(case when codtipocontribuinte is null then 1 else 0 end),
            sum(case when codtipocontribuinte = '' then 1 else 0 end),
            sum(
                case
                    when codtipocontribuinte is not null and codtipocontribuinte != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        -- views_nroCpf_indice_map_fields (analyzed below as potential non-scalar)
        select
            'codAtividade' as column_name,
            sum(case when codatividade is null then 1 else 0 end),
            sum(case when codatividade = '' then 1 else 0 end),
            sum(
                case
                    when codatividade is not null and codatividade != '' then 1 else 0
                end
            )
        from fonte_extracted
        union all
        select
            'nomeCpf' as column_name,
            sum(case when nomecpf is null then 1 else 0 end),
            sum(case when nomecpf = '' then 1 else 0 end),
            sum(case when nomecpf is not null and nomecpf != '' then 1 else 0 end)
        from fonte_extracted
        union all
        select
            'nomeQualificacao' as column_name,
            sum(case when nomequalificacao is null then 1 else 0 end),
            sum(case when nomequalificacao = '' then 1 else 0 end),
            sum(
                case
                    when nomequalificacao is not null and nomequalificacao != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'language' as column_name,
            sum(case when language is null then 1 else 0 end),
            sum(case when language = '' then 1 else 0 end),
            sum(case when language is not null and language != '' then 1 else 0 end)
        from fonte_extracted
        union all
        select
            'codOrgaoMunicipio' as column_name,
            sum(case when codorgaomunicipio is null then 1 else 0 end),
            sum(case when codorgaomunicipio = '' then 1 else 0 end),
            sum(
                case
                    when codorgaomunicipio is not null and codorgaomunicipio != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'codSituacaoCpf' as column_name,
            sum(case when codsituacaocpf is null then 1 else 0 end),
            sum(case when codsituacaocpf = '' then 1 else 0 end),
            sum(
                case
                    when codsituacaocpf is not null and codsituacaocpf != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'ufMunicipio' as column_name,
            sum(case when ufmunicipio is null then 1 else 0 end),
            sum(case when ufmunicipio = '' then 1 else 0 end),
            sum(
                case
                    when ufmunicipio is not null and ufmunicipio != '' then 1 else 0
                end
            )
        from fonte_extracted
        union all
        -- views_nroCpf_indice (analyzed below as potential non-scalar)
        select
            'timestamp' as column_name,
            sum(case when timestamp is null then 1 else 0 end),
            sum(case when timestamp = '' then 1 else 0 end),
            sum(case when timestamp is not null and timestamp != '' then 1 else 0 end)
        from fonte_extracted
        union all
        -- views_nroCpf_indice_map (analyzed below as potential non-scalar)
        select
            '_id' as column_name,
            sum(case when _id is null then 1 else 0 end),
            sum(case when _id = '' then 1 else 0 end),
            sum(case when _id is not null and _id != '' then 1 else 0 end)
        from fonte_extracted
        union all
        -- cnaes (analyzed below as array)
        select
            'emailContato' as column_name,
            sum(case when emailcontato is null then 1 else 0 end),
            sum(case when emailcontato = '' then 1 else 0 end),
            sum(
                case
                    when emailcontato is not null and emailcontato != '' then 1 else 0
                end
            )
        from fonte_extracted
        union all
        select
            'nomeSituacao' as column_name,
            sum(case when nomesituacao is null then 1 else 0 end),
            sum(case when nomesituacao = '' then 1 else 0 end),
            sum(
                case
                    when nomesituacao is not null and nomesituacao != '' then 1 else 0
                end
            )
        from fonte_extracted

        union all
        select
            '_rev' as column_name,
            sum(case when _rev is null then 1 else 0 end),
            sum(case when _rev = '' then 1 else 0 end),
            sum(case when _rev is not null and _rev != '' then 1 else 0 end)
        from fonte_extracted
        union all
        select
            'cei' as column_name,
            sum(case when cei is null then 1 else 0 end),
            sum(case when cei = '' then 1 else 0 end),
            sum(case when cei is not null and cei != '' then 1 else 0 end)
        from fonte_extracted
        union all
        select
            'codMunicipio' as column_name,
            sum(case when codmunicipio is null then 1 else 0 end),
            sum(case when codmunicipio = '' then 1 else 0 end),
            sum(
                case
                    when codmunicipio is not null and codmunicipio != '' then 1 else 0
                end
            )
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
            'nroAepf' as column_name,
            sum(case when nroaepf is null then 1 else 0 end),
            sum(case when nroaepf = '' then 1 else 0 end),
            sum(case when nroaepf is not null and nroaepf != '' then 1 else 0 end)
        from fonte_extracted
        union all
        -- views (analyzed below as potential non-scalar)
        select
            'codQualificacao' as column_name,
            sum(case when codqualificacao is null then 1 else 0 end),
            sum(case when codqualificacao = '' then 1 else 0 end),
            sum(
                case
                    when codqualificacao is not null and codqualificacao != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all

        -- --- Source Column (id) ---
        -- Assuming 'id' is not a string where '' check makes sense (e.g., BYTES,
        -- INT64). NULL vs NOT NULL only.
        -- --- ARRAY Columns ---
        select
            'cnaes' as column_name,
            sum(case when cnaes is null then 1 else 0 end) as null_count,
            sum(case when array_length(cnaes) = 0 then 1 else 0 end) as empty_count,
            sum(
                case
                    when cnaes is not null and array_length(cnaes) > 0 then 1 else 0
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
