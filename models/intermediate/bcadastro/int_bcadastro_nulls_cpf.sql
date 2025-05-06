{{
    config(
        alias="nulls_cpf",
        schema='debug_bcadastro',
        materialized=('table' if target.name == 'dev' else 'ephemeral'),
    )
}}

with
    -- Step 1: Get the total number of rows efficiently
    total_row_count as (
        select count(*) as total_rows
        from `rj-iplanrio.brutos_bcadastro_staging.chcpf_bcadastros`  -- Target table for CPF
    ),
    -- Step 2: Parse the JSON and extract fields. Use aliases for internal CTE use.
    -- Apply SAFE.PARSE_DATE where appropriate to treat dates as DATE type.
    fonte_extracted as (
        select
            json_value(doc, '$.cpfId') as cpf_id,

            -- Foreign keys (extracted as STRING)
            json_value(doc, '$.codMunDomic') as cod_mun_domic_raw,
            json_value(doc, '$.codMunNat') as cod_mun_nat_raw,
            json_value(doc, '$.codPaisNac') as cod_pais_nac_raw,
            json_value(doc, '$.codPaisRes') as cod_pais_res_raw,
            json_value(doc, '$.codNatOcup') as cod_nat_ocup_raw,
            json_value(doc, '$.codOcup') as cod_ocup_raw,
            json_value(doc, '$.codUA') as cod_ua_raw,

            -- Person data (extracted as STRING)
            json_value(doc, '$.nomeContribuinte') as nome,
            json_value(doc, '$.nomeSocial') as nome_social,
            json_value(doc, '$.nomeMae') as mae_nome,

            -- Dates (extracted and parsed as DATE or NULL)
            json_value(doc, '$.dtNasc') as nascimento_data,
            json_value(doc, '$.dtInscricao') as inscricao_data,
            json_value(doc, '$.dtUltAtualiz') as atualizacao_data,

            -- Status and demographics (extracted as STRING)
            json_value(doc, '$.codSitCad') as cod_sit_cad_raw,
            json_value(doc, '$.codSexo') as cod_sexo_raw,
            json_value(doc, '$.anoObito') as obito_ano_raw,
            json_value(doc, '$.indEstrangeiro') as indicativo_estrangeiro_raw,
            json_value(doc, '$.indResExt') as indicativo_residente_exterior_raw,

            -- Contact (extracted as STRING)
            json_value(doc, '$.telefone') as telefone,
            json_value(doc, '$.email') as email,

            -- Address (extracted as STRING)
            json_value(doc, '$.cep') as endereco_cep,
            json_value(doc, '$.ufMunDomic') as endereco_uf,
            json_value(doc, '$.bairro') as endereco_bairro,
            json_value(doc, '$.tipoLogradouro') as endereco_tipo_logradouro,
            json_value(doc, '$.logradouro') as endereco_logradouro,
            json_value(doc, '$.nroLogradouro') as endereco_numero,
            json_value(doc, '$.complemento') as endereco_complemento,
            json_value(doc, '$.municipio') as endereco_municipio,

            -- Birth and residence (extracted as STRING)
            json_value(doc, '$.ufMunNat') as nascimento_uf,
            json_value(doc, '$.nomePaisNac') as nascimento_pais,
            json_value(doc, '$.nomePaisRes') as residencia_pais,

            -- Metadata (extracted as STRING, handling version string replace)
            json_value(doc, '$.anoExerc') as exercicio_ano_raw,
            json_value(replace(to_json_string(doc), '~', ''), '$.version') as version,  -- Extracted value for ~version
            json_value(doc, '$.tipo') as tipo,
            json_value(doc, '$.timestamp') as timestamp,

            -- _id, _rev from doc
            json_value(doc, '$._id') as _id,
            json_value(doc, '$._rev') as _rev

        from `rj-iplanrio.brutos_bcadastro_staging.chcpf_bcadastros`
    ),

    -- Step 3: Calculate counts for each column using UNION ALL.
    -- Use original JSON key names or source column names as column_name literals.
    -- Reference the aliased names from fonte_extracted for the CASE logic.
    column_counts as (
        -- --- STRING Columns ---
        select
            'cpfId' as column_name,
            sum(case when cpf_id is null then 1 else 0 end) as null_count,
            sum(case when cpf_id = '' then 1 else 0 end) as empty_count,
            sum(
                case when cpf_id is not null and cpf_id != '' then 1 else 0 end
            ) as other_count
        from fonte_extracted
        union all
        select
            'codMunDomic' as column_name,
            sum(case when cod_mun_domic_raw is null then 1 else 0 end),
            sum(case when cod_mun_domic_raw = '' then 1 else 0 end),
            sum(
                case
                    when cod_mun_domic_raw is not null and cod_mun_domic_raw != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'codMunNat' as column_name,
            sum(case when cod_mun_nat_raw is null then 1 else 0 end),
            sum(case when cod_mun_nat_raw = '' then 1 else 0 end),
            sum(
                case
                    when cod_mun_nat_raw is not null and cod_mun_nat_raw != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'codPaisNac' as column_name,
            sum(case when cod_pais_nac_raw is null then 1 else 0 end),
            sum(case when cod_pais_nac_raw = '' then 1 else 0 end),
            sum(
                case
                    when cod_pais_nac_raw is not null and cod_pais_nac_raw != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'codPaisRes' as column_name,
            sum(case when cod_pais_res_raw is null then 1 else 0 end),
            sum(case when cod_pais_res_raw = '' then 1 else 0 end),
            sum(
                case
                    when cod_pais_res_raw is not null and cod_pais_res_raw != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'codNatOcup' as column_name,
            sum(case when cod_nat_ocup_raw is null then 1 else 0 end),
            sum(case when cod_nat_ocup_raw = '' then 1 else 0 end),
            sum(
                case
                    when cod_nat_ocup_raw is not null and cod_nat_ocup_raw != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'codOcup' as column_name,
            sum(case when cod_ocup_raw is null then 1 else 0 end),
            sum(case when cod_ocup_raw = '' then 1 else 0 end),
            sum(
                case
                    when cod_ocup_raw is not null and cod_ocup_raw != '' then 1 else 0
                end
            )
        from fonte_extracted
        union all
        select
            'codUA' as column_name,
            sum(case when cod_ua_raw is null then 1 else 0 end),
            sum(case when cod_ua_raw = '' then 1 else 0 end),
            sum(case when cod_ua_raw is not null and cod_ua_raw != '' then 1 else 0 end)
        from fonte_extracted
        union all
        select
            'nomeContribuinte' as column_name,
            sum(case when nome is null then 1 else 0 end),
            sum(case when nome = '' then 1 else 0 end),
            sum(case when nome is not null and nome != '' then 1 else 0 end)
        from fonte_extracted
        union all
        select
            'nomeSocial' as column_name,
            sum(case when nome_social is null then 1 else 0 end),
            sum(case when nome_social = '' then 1 else 0 end),
            sum(
                case
                    when nome_social is not null and nome_social != '' then 1 else 0
                end
            )
        from fonte_extracted
        union all
        select
            'nomeMae' as column_name,
            sum(case when mae_nome is null then 1 else 0 end),
            sum(case when mae_nome = '' then 1 else 0 end),
            sum(case when mae_nome is not null and mae_nome != '' then 1 else 0 end)
        from fonte_extracted
        union all
        select
            'codSitCad' as column_name,
            sum(case when cod_sit_cad_raw is null then 1 else 0 end),
            sum(case when cod_sit_cad_raw = '' then 1 else 0 end),
            sum(
                case
                    when cod_sit_cad_raw is not null and cod_sit_cad_raw != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'codSexo' as column_name,
            sum(case when cod_sexo_raw is null then 1 else 0 end),
            sum(case when cod_sexo_raw = '' then 1 else 0 end),
            sum(
                case
                    when cod_sexo_raw is not null and cod_sexo_raw != '' then 1 else 0
                end
            )
        from fonte_extracted
        union all
        select
            'anoObito' as column_name,
            sum(case when obito_ano_raw is null then 1 else 0 end),
            sum(case when obito_ano_raw = '' then 1 else 0 end),
            sum(
                case
                    when obito_ano_raw is not null and obito_ano_raw != '' then 1 else 0
                end
            )
        from fonte_extracted
        union all
        select
            'indEstrangeiro' as column_name,
            sum(case when indicativo_estrangeiro_raw is null then 1 else 0 end),
            sum(case when indicativo_estrangeiro_raw = '' then 1 else 0 end),
            sum(
                case
                    when
                        indicativo_estrangeiro_raw is not null
                        and indicativo_estrangeiro_raw != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'indResExt' as column_name,
            sum(case when indicativo_residente_exterior_raw is null then 1 else 0 end),
            sum(case when indicativo_residente_exterior_raw = '' then 1 else 0 end),
            sum(
                case
                    when
                        indicativo_residente_exterior_raw is not null
                        and indicativo_residente_exterior_raw != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'telefone' as column_name,
            sum(case when telefone is null then 1 else 0 end),
            sum(case when telefone = '' then 1 else 0 end),
            sum(case when telefone is not null and telefone != '' then 1 else 0 end)
        from fonte_extracted
        union all
        select
            'email' as column_name,
            sum(case when email is null then 1 else 0 end),
            sum(case when email = '' then 1 else 0 end),
            sum(case when email is not null and email != '' then 1 else 0 end)
        from fonte_extracted
        union all
        select
            'cep' as column_name,
            sum(case when endereco_cep is null then 1 else 0 end),
            sum(case when endereco_cep = '' then 1 else 0 end),
            sum(
                case
                    when endereco_cep is not null and endereco_cep != '' then 1 else 0
                end
            )
        from fonte_extracted
        union all
        select
            'ufMunDomic' as column_name,
            sum(case when endereco_uf is null then 1 else 0 end),
            sum(case when endereco_uf = '' then 1 else 0 end),
            sum(
                case
                    when endereco_uf is not null and endereco_uf != '' then 1 else 0
                end
            )
        from fonte_extracted
        union all
        select
            'bairro' as column_name,
            sum(case when endereco_bairro is null then 1 else 0 end),
            sum(case when endereco_bairro = '' then 1 else 0 end),
            sum(
                case
                    when endereco_bairro is not null and endereco_bairro != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'tipoLogradouro' as column_name,
            sum(case when endereco_tipo_logradouro is null then 1 else 0 end),
            sum(case when endereco_tipo_logradouro = '' then 1 else 0 end),
            sum(
                case
                    when
                        endereco_tipo_logradouro is not null
                        and endereco_tipo_logradouro != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'logradouro' as column_name,
            sum(case when endereco_logradouro is null then 1 else 0 end),
            sum(case when endereco_logradouro = '' then 1 else 0 end),
            sum(
                case
                    when endereco_logradouro is not null and endereco_logradouro != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'nroLogradouro' as column_name,
            sum(case when endereco_numero is null then 1 else 0 end),
            sum(case when endereco_numero = '' then 1 else 0 end),
            sum(
                case
                    when endereco_numero is not null and endereco_numero != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'complemento' as column_name,
            sum(case when endereco_complemento is null then 1 else 0 end),
            sum(case when endereco_complemento = '' then 1 else 0 end),
            sum(
                case
                    when endereco_complemento is not null and endereco_complemento != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'municipio' as column_name,
            sum(case when endereco_municipio is null then 1 else 0 end),
            sum(case when endereco_municipio = '' then 1 else 0 end),
            sum(
                case
                    when endereco_municipio is not null and endereco_municipio != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'ufMunNat' as column_name,
            sum(case when nascimento_uf is null then 1 else 0 end),
            sum(case when nascimento_uf = '' then 1 else 0 end),
            sum(
                case
                    when nascimento_uf is not null and nascimento_uf != '' then 1 else 0
                end
            )
        from fonte_extracted
        union all
        select
            'nomePaisNac' as column_name,
            sum(case when nascimento_pais is null then 1 else 0 end),
            sum(case when nascimento_pais = '' then 1 else 0 end),
            sum(
                case
                    when nascimento_pais is not null and nascimento_pais != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'nomePaisRes' as column_name,
            sum(case when residencia_pais is null then 1 else 0 end),
            sum(case when residencia_pais = '' then 1 else 0 end),
            sum(
                case
                    when residencia_pais is not null and residencia_pais != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'anoExerc' as column_name,
            sum(case when exercicio_ano_raw is null then 1 else 0 end),
            sum(case when exercicio_ano_raw = '' then 1 else 0 end),
            sum(
                case
                    when exercicio_ano_raw is not null and exercicio_ano_raw != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            '~version' as column_name,
            sum(case when version is null then 1 else 0 end),
            sum(case when version = '' then 1 else 0 end),
            sum(case when version is not null and version != '' then 1 else 0 end)
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
            'timestamp' as column_name,
            sum(case when timestamp is null then 1 else 0 end),
            sum(case when timestamp = '' then 1 else 0 end),
            sum(case when timestamp is not null and timestamp != '' then 1 else 0 end)
        from fonte_extracted
        union all  -- 'rev' from value column
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

        -- --- DATE Columns (NULL vs NOT NULL only, empty_count is 0) ---
        select
            'dtNasc' as column_name,
            sum(case when nascimento_data is null then 1 else 0 end) as null_count,
            0 as empty_count,
            sum(case when nascimento_data is not null then 1 else 0 end) as other_count
        from fonte_extracted
        union all
        select
            'dtInscricao' as column_name,
            sum(case when inscricao_data is null then 1 else 0 end),
            0 as empty_count,
            sum(case when inscricao_data is not null then 1 else 0 end)
        from fonte_extracted
        union all
        select
            'dtUltAtualiz' as column_name,
            sum(case when atualizacao_data is null then 1 else 0 end),
            0 as empty_count,
            sum(case when atualizacao_data is not null then 1 else 0 end)
        from fonte_extracted

    -- UNION ALL blocks for other types (ARRAY, STRUCT, etc.) would go here if present
    -- in the extracted list
    -- No Arrays or complex structs in the user's provided list for this query.
    )

-- Step 4: Calculate percentages by joining counts with total count
select
    cc.column_name as key,  -- Use the original JSON key name from column_counts
    round(safe_divide(cc.null_count * 100, trc.total_rows), 2) as null_percent,
    round(safe_divide(cc.empty_count * 100, trc.total_rows), 2) as empty_percent,
    round(safe_divide(cc.other_count * 100, trc.total_rows), 2) as others_percent  -- Non-null, non-empty
from column_counts as cc
cross join  -- Joins each row from column_counts with the single row from total_row_count
    total_row_count as trc
order by null_percent desc  -- Order by null percentage descending
