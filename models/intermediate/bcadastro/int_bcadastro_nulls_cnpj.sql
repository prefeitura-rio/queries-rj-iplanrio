{{
    config(
        alias="nulls_cnpj",
        schema='debug_bcadastro',
        materialized=('table' if target.name == 'dev' else 'ephemeral'),
    )
}}

with
    -- Step 1: Get the total number of rows efficiently
    total_row_count as (
        select count(*) as total_rows
        from `rj-iplanrio.brutos_bcadastro_staging.chcnpj_bcadastros`
    ),
    -- Step 2: Parse the JSON and extract fields. Aliases are used for convenience
    -- within this CTE, but the original JSON key names will be used in the output.
    fonte_extracted as (
        select
            -- Primary key (extracted as STRING)
            json_value(doc, '$.cnpj') as cnpj,

            -- Foreign keys (extracted as STRING initially)
            json_value(doc, '$.codigoMunicipio') as codigo_municipio,
            json_value(doc, '$.codigoPais') as codigo_pais,
            json_value(doc, '$.naturezaJuridica') as natureza_juridica,
            json_value(doc, '$.qualificacaoResponsavel') as qualificacao_responsavel,
            json_value(doc, '$.porteEmpresa') as porte_empresa,
            json_value(doc, '$.indicadorMatriz') as indicador_matriz,
            json_value(doc, '$.tipoOrgaoRegistro') as tipo_orgao_registro,
            json_value(doc, '$.motivoSituacao') as motivo_situacao,
            json_value(doc, '$.situacaoCadastral') as situacao_cadastral,

            -- Business data (extracted as STRING)
            json_value(doc, '$.nomeEmpresarial') as nome_empresarial,
            json_value(doc, '$.nomeFantasia') as nome_fantasia,
            json_value(doc, '$.capitalSocial') as capital_social,
            json_value(doc, '$.cnaeFiscal') as cnae_fiscal,
            json_value(doc, '$.nire') as nire,
            json_value(doc, '$.cnpjSucedida') as cnpj_sucedida,

            -- Dates (extracted as STRING here, conversion to DATE happens later if
            -- needed for analysis)
            json_value(doc, '$.dataInicioAtividade') as data_inicio_atividade,
            json_value(doc, '$.dataSituacaoCadastral') as data_situacao_cadastral,
            json_value(doc, '$.dataSituacaoEspecial') as data_situacao_especial,
            json_value(doc, '$.dataInclusaoResponsavel') as data_inclusao_responsavel,

            -- Status and demographics (extracted as STRING, handling enteFederativo
            -- logic)
            json_value(doc, '$.situacaoEspecial') as situacao_especial,
            case
                when regexp_contains(json_value(doc, '$.enteFederativo'), r'^[0-9]+$')
                then json_value(doc, '$.enteFederativo')
                else upper(json_value(doc, '$.enteFederativo'))
            end as id_ente_federativo,  -- Analyzing the result of this case

            -- Contact (extracted as STRING)
            json_value(doc, '$.dddTelefone1') as ddd_telefone_1,
            json_value(doc, '$.telefone1') as telefone_1,
            json_value(doc, '$.dddTelefone2') as ddd_telefone_2,
            json_value(doc, '$.telefone2') as telefone_2,
            json_value(doc, '$.email') as email,

            -- Address (extracted as STRING)
            json_value(doc, '$.cep') as endereco_cep,  -- Assuming this corresponds to the $.cep key
            json_value(doc, '$.uf') as endereco_uf,  -- Assuming this corresponds to the $.uf key
            json_value(doc, '$.bairro') as endereco_bairro,  -- Assuming this corresponds to the $.bairro key
            json_value(doc, '$.tipoLogradouro') as endereco_tipo_logradouro,  -- Assuming this corresponds to the $.tipoLogradouro key
            json_value(doc, '$.logradouro') as endereco_logradouro,  -- Assuming this corresponds to the $.logradouro key
            json_value(doc, '$.numero') as endereco_numero,  -- Assuming this corresponds to the $.numero key
            json_value(doc, '$.complemento') as endereco_complemento,  -- Assuming this corresponds to the $.complemento key
            json_value(doc, '$.nomeCidadeExterior') as endereco_nome_cidade_exterior,  -- Assuming this corresponds to the $.nomeCidadeExterior key

            -- Accountant Information (extracted as STRING)
            json_value(doc, '$.tipoCrcContadorPF') as tipo_crc_contador_pf,
            json_value(doc, '$.contadorPJ') as contador_pj,
            json_value(
                doc, '$.classificacaoCrcContadorPF'
            ) as classificacao_crc_contador_pf,
            json_value(doc, '$.sequencialCrcContadorPF') as sequencial_crc_contador_pf,
            json_value(doc, '$.contadorPF') as contador_pf,
            json_value(doc, '$.tipoCrcContadorPJ') as tipo_crc_contador_pj,
            json_value(
                doc, '$.classificacaoCrcContadorPJ'
            ) as classificacao_crc_contador_pj,
            json_value(doc, '$.ufCrcContadorPJ') as uf_crc_contador_pj,
            json_value(doc, '$.ufCrcContadorPF') as uf_crc_contador_pf,
            json_value(doc, '$.sequencialCrcContadorPJ') as sequencial_crc_contador_pj,

            -- Responsible Person (extracted as STRING)
            json_value(doc, '$.cpfResponsavel') as cpf_responsavel,

            -- Arrays (extracted as ARRAY or NULL)
            json_extract_array(doc, '$.cnaeSecundarias') as cnae_secundarias,
            json_extract_array(doc, '$.tiposUnidade') as tipos_unidade,
            json_extract_array(doc, '$.formasAtuacao') as formas_atuacao,
            json_extract_array(doc, '$.socios') as socios,
            json_extract_array(doc, '$.sucessoes') as sucessoes,

            -- Metadata (extracted as STRING, handling version string replace)
            json_value(doc, '$.timestamp') as timestamp,
            json_value(doc, '$.language') as language,
            -- Use the original key name including the '~' if that's what you want
            -- reported
            json_value(
                replace(to_json_string(doc), '~', ''), '$.version'
            ) as version_extracted,  -- Alias the extracted value

            -- Other potential fields from doc (assuming based on original CTE
            -- structure)
            json_value(doc, '$._id') as _id,
            json_value(doc, '$._rev') as _rev,
        -- Assuming 'id' extracted from doc maps to key 'id'
        from `rj-iplanrio.brutos_bcadastro_staging.chcnpj_bcadastros`
    ),

    -- Step 3: Calculate counts for each column using UNION ALL
    column_counts as (
        -- For each column extracted from 'doc' in fonte_extracted, create a SELECT
        -- statement
        -- Use the original JSON key name as the 'column_name' literal string
        -- STRING Columns
        select
            'cnpj' as column_name,
            sum(case when cnpj is null then 1 else 0 end) as null_count,
            sum(case when cnpj = '' then 1 else 0 end) as empty_count,
            sum(
                case when cnpj is not null and cnpj != '' then 1 else 0 end
            ) as other_count
        from fonte_extracted
        union all
        select
            'codigoMunicipio' as column_name,
            sum(case when codigo_municipio is null then 1 else 0 end),
            sum(case when codigo_municipio = '' then 1 else 0 end),
            sum(
                case
                    when codigo_municipio is not null and codigo_municipio != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'codigoPais' as column_name,
            sum(case when codigo_pais is null then 1 else 0 end),
            sum(case when codigo_pais = '' then 1 else 0 end),
            sum(
                case
                    when codigo_pais is not null and codigo_pais != '' then 1 else 0
                end
            )
        from fonte_extracted
        union all
        select
            'naturezaJuridica' as column_name,
            sum(case when natureza_juridica is null then 1 else 0 end),
            sum(case when natureza_juridica = '' then 1 else 0 end),
            sum(
                case
                    when natureza_juridica is not null and natureza_juridica != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'qualificacaoResponsavel' as column_name,
            sum(case when qualificacao_responsavel is null then 1 else 0 end),
            sum(case when qualificacao_responsavel = '' then 1 else 0 end),
            sum(
                case
                    when
                        qualificacao_responsavel is not null
                        and qualificacao_responsavel != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'porteEmpresa' as column_name,
            sum(case when porte_empresa is null then 1 else 0 end),
            sum(case when porte_empresa = '' then 1 else 0 end),
            sum(
                case
                    when porte_empresa is not null and porte_empresa != '' then 1 else 0
                end
            )
        from fonte_extracted
        union all
        select
            'indicadorMatriz' as column_name,
            sum(case when indicador_matriz is null then 1 else 0 end),
            sum(case when indicador_matriz = '' then 1 else 0 end),
            sum(
                case
                    when indicador_matriz is not null and indicador_matriz != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'tipoOrgaoRegistro' as column_name,
            sum(case when tipo_orgao_registro is null then 1 else 0 end),
            sum(case when tipo_orgao_registro = '' then 1 else 0 end),
            sum(
                case
                    when tipo_orgao_registro is not null and tipo_orgao_registro != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'motivoSituacao' as column_name,
            sum(case when motivo_situacao is null then 1 else 0 end),
            sum(case when motivo_situacao = '' then 1 else 0 end),
            sum(
                case
                    when motivo_situacao is not null and motivo_situacao != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'situacaoCadastral' as column_name,
            sum(case when situacao_cadastral is null then 1 else 0 end),
            sum(case when situacao_cadastral = '' then 1 else 0 end),
            sum(
                case
                    when situacao_cadastral is not null and situacao_cadastral != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        -- Analyze the derived id_ente_federativo, report under original key
        -- 'enteFederativo'
        select
            'enteFederativo' as column_name,
            sum(case when id_ente_federativo is null then 1 else 0 end),
            sum(case when id_ente_federativo = '' then 1 else 0 end),
            sum(
                case
                    when id_ente_federativo is not null and id_ente_federativo != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'nomeEmpresarial' as column_name,
            sum(case when nome_empresarial is null then 1 else 0 end),
            sum(case when nome_empresarial = '' then 1 else 0 end),
            sum(
                case
                    when nome_empresarial is not null and nome_empresarial != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'nomeFantasia' as column_name,
            sum(case when nome_fantasia is null then 1 else 0 end),
            sum(case when nome_fantasia = '' then 1 else 0 end),
            sum(
                case
                    when nome_fantasia is not null and nome_fantasia != '' then 1 else 0
                end
            )
        from fonte_extracted
        union all
        select
            'capitalSocial' as column_name,
            sum(case when capital_social is null then 1 else 0 end),
            sum(case when capital_social = '' then 1 else 0 end),
            sum(
                case
                    when capital_social is not null and capital_social != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'cnaeFiscal' as column_name,
            sum(case when cnae_fiscal is null then 1 else 0 end),
            sum(case when cnae_fiscal = '' then 1 else 0 end),
            sum(
                case
                    when cnae_fiscal is not null and cnae_fiscal != '' then 1 else 0
                end
            )
        from fonte_extracted
        union all
        select
            'nire' as column_name,
            sum(case when nire is null then 1 else 0 end),
            sum(case when nire = '' then 1 else 0 end),
            sum(case when nire is not null and nire != '' then 1 else 0 end)
        from fonte_extracted
        union all
        select
            'cnpjSucedida' as column_name,
            sum(case when cnpj_sucedida is null then 1 else 0 end),
            sum(case when cnpj_sucedida = '' then 1 else 0 end),
            sum(
                case
                    when cnpj_sucedida is not null and cnpj_sucedida != '' then 1 else 0
                end
            )
        from fonte_extracted
        union all

        -- Date fields (extracted as strings, analyze as NULL vs NOT NULL)
        select
            'dataInicioAtividade' as column_name,
            sum(
                case when data_inicio_atividade is null then 1 else 0 end
            ) as null_count,
            0 as empty_count,
            sum(
                case when data_inicio_atividade is not null then 1 else 0 end
            ) as other_count
        from fonte_extracted
        union all
        select
            'dataSituacaoCadastral' as column_name,
            sum(case when data_situacao_cadastral is null then 1 else 0 end),
            0 as empty_count,
            sum(case when data_situacao_cadastral is not null then 1 else 0 end)
        from fonte_extracted
        union all
        select
            'dataSituacaoEspecial' as column_name,
            sum(case when data_situacao_especial is null then 1 else 0 end),
            0 as empty_count,
            sum(case when data_situacao_especial is not null then 1 else 0 end)
        from fonte_extracted
        union all
        select
            'dataInclusaoResponsavel' as column_name,
            sum(case when data_inclusao_responsavel is null then 1 else 0 end),
            0 as empty_count,
            sum(case when data_inclusao_responsavel is not null then 1 else 0 end)
        from fonte_extracted
        union all

        -- Remaining String Columns
        select
            'situacaoEspecial' as column_name,
            sum(case when situacao_especial is null then 1 else 0 end),
            sum(case when situacao_especial = '' then 1 else 0 end),
            sum(
                case
                    when situacao_especial is not null and situacao_especial != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'dddTelefone1' as column_name,
            sum(case when ddd_telefone_1 is null then 1 else 0 end),
            sum(case when ddd_telefone_1 = '' then 1 else 0 end),
            sum(
                case
                    when ddd_telefone_1 is not null and ddd_telefone_1 != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'telefone1' as column_name,
            sum(case when telefone_1 is null then 1 else 0 end),
            sum(case when telefone_1 = '' then 1 else 0 end),
            sum(case when telefone_1 is not null and telefone_1 != '' then 1 else 0 end)
        from fonte_extracted
        union all
        select
            'dddTelefone2' as column_name,
            sum(case when ddd_telefone_2 is null then 1 else 0 end),
            sum(case when ddd_telefone_2 = '' then 1 else 0 end),
            sum(
                case
                    when ddd_telefone_2 is not null and ddd_telefone_2 != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'telefone2' as column_name,
            sum(case when telefone_2 is null then 1 else 0 end),
            sum(case when telefone_2 = '' then 1 else 0 end),
            sum(case when telefone_2 is not null and telefone_2 != '' then 1 else 0 end)
        from fonte_extracted
        union all
        select
            'email' as column_name,
            sum(case when email is null then 1 else 0 end),
            sum(case when email = '' then 1 else 0 end),
            sum(case when email is not null and email != '' then 1 else 0 end)
        from fonte_extracted
        union all
        -- Address fields (using original JSON keys)
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
            'uf' as column_name,
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
            'numero' as column_name,
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
            'nomeCidadeExterior' as column_name,
            sum(case when endereco_nome_cidade_exterior is null then 1 else 0 end),
            sum(case when endereco_nome_cidade_exterior = '' then 1 else 0 end),
            sum(
                case
                    when
                        endereco_nome_cidade_exterior is not null
                        and endereco_nome_cidade_exterior != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        -- Accountant Information (using original JSON keys)
        select
            'tipoCrcContadorPF' as column_name,
            sum(case when tipo_crc_contador_pf is null then 1 else 0 end),
            sum(case when tipo_crc_contador_pf = '' then 1 else 0 end),
            sum(
                case
                    when tipo_crc_contador_pf is not null and tipo_crc_contador_pf != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'contadorPJ' as column_name,
            sum(case when contador_pj is null then 1 else 0 end),
            sum(case when contador_pj = '' then 1 else 0 end),
            sum(
                case
                    when contador_pj is not null and contador_pj != '' then 1 else 0
                end
            )
        from fonte_extracted
        union all
        select
            'classificacaoCrcContadorPF' as column_name,
            sum(case when classificacao_crc_contador_pf is null then 1 else 0 end),
            sum(case when classificacao_crc_contador_pf = '' then 1 else 0 end),
            sum(
                case
                    when
                        classificacao_crc_contador_pf is not null
                        and classificacao_crc_contador_pf != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'sequencialCrcContadorPF' as column_name,
            sum(case when sequencial_crc_contador_pf is null then 1 else 0 end),
            sum(case when sequencial_crc_contador_pf = '' then 1 else 0 end),
            sum(
                case
                    when
                        sequencial_crc_contador_pf is not null
                        and sequencial_crc_contador_pf != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'contadorPF' as column_name,
            sum(case when contador_pf is null then 1 else 0 end),
            sum(case when contador_pf = '' then 1 else 0 end),
            sum(
                case
                    when contador_pf is not null and contador_pf != '' then 1 else 0
                end
            )
        from fonte_extracted
        union all
        select
            'tipoCrcContadorPJ' as column_name,
            sum(case when tipo_crc_contador_pj is null then 1 else 0 end),
            sum(case when tipo_crc_contador_pj = '' then 1 else 0 end),
            sum(
                case
                    when tipo_crc_contador_pj is not null and tipo_crc_contador_pj != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'classificacaoCrcContadorPJ' as column_name,
            sum(case when classificacao_crc_contador_pj is null then 1 else 0 end),
            sum(case when classificacao_crc_contador_pj = '' then 1 else 0 end),
            sum(
                case
                    when
                        classificacao_crc_contador_pj is not null
                        and classificacao_crc_contador_pj != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'ufCrcContadorPJ' as column_name,
            sum(case when uf_crc_contador_pj is null then 1 else 0 end),
            sum(case when uf_crc_contador_pj = '' then 1 else 0 end),
            sum(
                case
                    when uf_crc_contador_pj is not null and uf_crc_contador_pj != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'ufCrcContadorPF' as column_name,
            sum(case when uf_crc_contador_pf is null then 1 else 0 end),
            sum(case when uf_crc_contador_pf = '' then 1 else 0 end),
            sum(
                case
                    when uf_crc_contador_pf is not null and uf_crc_contador_pf != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'sequencialCrcContadorPJ' as column_name,
            sum(case when sequencial_crc_contador_pj is null then 1 else 0 end),
            sum(case when sequencial_crc_contador_pj = '' then 1 else 0 end),
            sum(
                case
                    when
                        sequencial_crc_contador_pj is not null
                        and sequencial_crc_contador_pj != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        -- Responsible Person (using original JSON key)
        select
            'cpfResponsavel' as column_name,
            sum(case when cpf_responsavel is null then 1 else 0 end),
            sum(case when cpf_responsavel = '' then 1 else 0 end),
            sum(
                case
                    when cpf_responsavel is not null and cpf_responsavel != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all

        -- Arrays (NULL vs Empty Array vs Non-empty Array) - Using original JSON keys
        select
            'cnaeSecundarias' as column_name,
            sum(case when cnae_secundarias is null then 1 else 0 end) as null_count,
            sum(
                case when array_length(cnae_secundarias) = 0 then 1 else 0 end
            ) as empty_count,
            sum(
                case
                    when
                        cnae_secundarias is not null
                        and array_length(cnae_secundarias) > 0
                    then 1
                    else 0
                end
            ) as other_count
        from fonte_extracted
        union all
        select
            'tiposUnidade' as column_name,
            sum(case when tipos_unidade is null then 1 else 0 end),
            sum(case when array_length(tipos_unidade) = 0 then 1 else 0 end),
            sum(
                case
                    when tipos_unidade is not null and array_length(tipos_unidade) > 0
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'formasAtuacao' as column_name,
            sum(case when formas_atuacao is null then 1 else 0 end),
            sum(case when array_length(formas_atuacao) = 0 then 1 else 0 end),
            sum(
                case
                    when formas_atuacao is not null and array_length(formas_atuacao) > 0
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all
        select
            'socios' as column_name,
            sum(case when socios is null then 1 else 0 end),
            sum(case when array_length(socios) = 0 then 1 else 0 end),
            sum(
                case
                    when socios is not null and array_length(socios) > 0 then 1 else 0
                end
            )
        from fonte_extracted
        union all
        select
            'sucessoes' as column_name,
            sum(case when sucessoes is null then 1 else 0 end),
            sum(case when array_length(sucessoes) = 0 then 1 else 0 end),
            sum(
                case
                    when sucessoes is not null and array_length(sucessoes) > 0
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all

        -- Metadata (string columns) - Using original JSON keys, report ~version as key
        select
            'timestamp' as column_name,
            sum(case when timestamp is null then 1 else 0 end),
            sum(case when timestamp = '' then 1 else 0 end),
            sum(case when timestamp is not null and timestamp != '' then 1 else 0 end)
        from fonte_extracted
        union all
        select
            'language' as column_name,
            sum(case when language is null then 1 else 0 end),
            sum(case when language = '' then 1 else 0 end),
            sum(case when language is not null and language != '' then 1 else 0 end)
        from fonte_extracted
        union all
        -- Use the original key name '~version' for the column name, but check the
        -- extracted value
        select
            '~version' as column_name,
            sum(case when version_extracted is null then 1 else 0 end),
            sum(case when version_extracted = '' then 1 else 0 end),
            sum(
                case
                    when version_extracted is not null and version_extracted != ''
                    then 1
                    else 0
                end
            )
        from fonte_extracted
        union all

        -- Other fields from doc (using original JSON keys)
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

    -- UNION ALL blocks for any source columns (like top-level id, key, rev from value)
    -- are excluded as per the request to use original *JSON* key names from 'doc'.
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
