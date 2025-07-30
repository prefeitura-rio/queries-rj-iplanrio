with
    -- SOURCES
    cnpj_base as (select * from {{ ref("int_bcadastro_chcnpj_parse") }}),

    sigla_uf_bd as (select sigla from {{ source("br_bd_diretorios_brasil", "uf") }}),

    municipio_bd as (
        select id_municipio_rf, nome as municipio_nome
        from {{ source("br_bd_diretorios_brasil", "municipio") }}
    ),

    dominio as (
        select id, {{ proper_br("descricao") }} as descricao, column
        from {{ source("brutos_bcadastro_staging", "dominio_cnpj") }}
    ),

    -- TRANSFORMATIONS
    cnpj_renomeada as (
        select
            -- Primary key
            cnpj as id_cnpj,

            -- Foreign keys
            codigomunicipio as id_municipio,
            codigopais as id_pais,
            naturezajuridica as id_natureza_juridica,
            indicadormatriz as id_indicador_matriz,
            tipoorgaoregistro as id_tipo_orgao_registro,
            motivosituacao as id_motivo_situacao,
            situacaocadastral as id_situacao_cadastral,
            porteempresa as id_porte,

            -- Business data
            nomeempresarial as razao_social,
            nomefantasia as nome_fantasia,
            capitalsocial as capital_social,
            cnae as cnae_fiscal,
            cnaesecundarias as cnae_secundarias,
            nire as nire,
            cnpjsucedida as id_cnpj_sucedida,

            -- Dates
            datainicioatividade as data_inicio_atividade,
            datasituacaocadastral as data_situacao_cadastral,
            datasituacaoespecial as data_situacao_especial,
            datainclusaoresponsavel as data_inclusao_responsavel,

            -- Status and demographics
            situacaoespecial as situacao_especial,
            entefederativo as id_ente_federativo,

            -- Contact
            dddtelefone1 as contato_ddd_1,
            telefone1 as contato_telefone_1,
            dddtelefone2 as contato_ddd_2,
            telefone2 as contato_telefone_2,
            email as contato_email,

            -- Address
            cep as endereco_cep,
            uf as endereco_uf,
            bairro as endereco_bairro,
            tipologradouro as endereco_tipo_logradouro,
            logradouro as endereco_logradouro,
            numero as endereco_numero,
            complemento as endereco_complemento,
            nomecidadeexterior as endereco_cidade_exterior_nome,

            -- Accountant Information
            tipocrccontadorpf as contador_pf_tipo_crc,
            contadorpj as contador_pj_id,
            classificacaocrccontadorpf as contador_pf_classificacao_crc,
            sequencialcrccontadorpf as contador_pf_sequencial_crc,
            contadorpf as contador_pf_id,
            tipocrccontadorpj as contador_pj_tipo_crc,
            classificacaocrccontadorpj as contador_pj_classificacao_crc,
            ufcrccontadorpj as contador_pj_uf_crc,
            ufcrccontadorpf as contador_pf_uf_crc,
            sequencialcrccontadorpj as contador_pj_sequencial_crc,
            cpfresponsavel as responsavel_cpf,
            qualificacaoresponsavel as id_qualificacao_responsavel,
            qualificacaoresponsavel as responsavel_qualificacao_descricao,

            -- Business arrays
            tiposunidade as tipos_unidade,
            formasatuacao as formas_atuacao,
            socios as socios,
            sucessoes as sucessoes,

            -- Metadata
            timestamp,
            language,
            version,

            -- Outros
            airbyte

        from cnpj_base
    ),

    array_convert_tb as (
        select
            id_cnpj,
            array_agg(distinct tut.descricao) as tipos_unidade,
            array_agg(distinct fat.descricao) as formas_atuacao
        from
            cnpj_renomeada t,
            unnest(t.formas_atuacao) as fa,
            unnest(t.tipos_unidade) as tu
        left join
            (
                select id as tipos_unidade_id, descricao
                from dominio
                where column = 'tipo_unidade'
            ) tut
            on cast(cast(json_value(tu) as int64) as string) = tut.tipos_unidade_id
        left join
            (
                select id as formas_atuacao_id, descricao
                from dominio
                where column = 'forma_atuacao'
            ) fat
            on cast(cast(json_value(fa) as int64) as string) = fat.formas_atuacao_id
        group by id_cnpj
    ),

    _socios_tb as (
        select
            id_cnpj,
            nullif(json_value(so, '$.codigoPais'), "") as codigo_pais,
            substr(nullif(json_value(so, '$.cpfCnpj'), ""), -11) as cpf_socio,
            nullif(json_value(so, '$.cpfCnpj'), "") as cnpj_socio,
            nullif(
                json_value(so, '$.cpfRepresentanteLegal'), ""
            ) as cpf_representante_legal,
            safe.parse_date(
                '%Y%m%d', nullif(json_value(so, '$.dataEntrada'), '')
            ) as data_situacao_especial,
            nullif(
                json_value(so, '$.nomeSocioEstrangeiro'), ""
            ) as nome_socio_estrangeiro,
            qrl.descricao as qualificacao_representante_legal,  -- qualificacao_representante_legal
            qs.descricao as qualificacao_socio,  -- qualificacao_socio
            ts.descricao as tipo  -- tipo_socio
        from cnpj_renomeada t, unnest(t.socios) as so
        left join
            (
                select id as qualificacao_representante_legal_id, descricao
                from dominio
                where column = 'qualificacao_representante_legal'
            ) qrl
            on cast(
                cast(
                    nullif(
                        json_value(so, '$.qualificacaoRepresentanteLegal'), ""
                    ) as int64
                ) as string
            )
            = qrl.qualificacao_representante_legal_id
        left join
            (
                select id as qualificacao_socio_id, descricao
                from dominio
                where column = 'qualificacao_socio'
            ) qs
            on cast(
                cast(
                    nullif(json_value(so, '$.qualificacaoSocio'), "") as int64
                ) as string
            )
            = qs.qualificacao_socio_id
        left join
            (
                select id as tipo_socio_id, descricao
                from dominio
                where column = 'tipo_socio'
            ) ts
            on cast(cast(nullif(json_value(so, '$.tipo'), "") as int64) as string)
            = ts.tipo_socio_id
    ),


    socios_tb as (
        select
            id_cnpj,
            array_agg(
                struct(
                    so.codigo_pais,
                    case
                        when cpf_valido_indicador = true then so.cpf_socio else null
                    end as cpf_socio,
                    case
                        when cpf_valido_indicador = false then so.cnpj_socio else null
                    end as cnpj_socio,
                    so.cpf_representante_legal,
                    so.data_situacao_especial,
                    so.nome_socio_estrangeiro,
                    so.qualificacao_representante_legal,
                    so.qualificacao_socio,
                    so.tipo
                )
            ) as socios
        from ( select
            id_cnpj,
            so.codigo_pais,
            so.cpf_socio,
            {{ validate_cpf("so.cpf_socio") }} as cpf_valido_indicador,
            so.cnpj_socio,
            so.cpf_representante_legal,
            so.data_situacao_especial,
            so.nome_socio_estrangeiro,
            so.qualificacao_representante_legal,
            so.qualificacao_socio,
            so.tipo
        from _socios_tb so) so
        group by id_cnpj
    ),

    sucessoes_tb as (
        select
            id_cnpj_sucedida,
            array_agg(
                struct(
                    ev.descricao as evento_sucedida,  -- evento
                    safe.parse_date(
                        '%Y%m%d', nullif(json_value(su, '$.dataEventoSucedida'), '')
                    ) as data_evento_sucedida,
                    safe.parse_date(
                        '%Y%m%d', nullif(json_value(su, '$.dataProcessamento'), '')
                    ) as data_processamento,
                    nullif(json_value(su, '$.sucessoras'), "") as sucessoras
                )
            ) as sucessoes
        from cnpj_renomeada t, unnest(t.sucessoes) as su
        left join
            (select id as evento_id, descricao from dominio where column = 'eventos') ev
            on cast(
                cast(
                    nullif(json_value(su, '$.codigoEventoSucedida'), "") as int64
                ) as string
            )
            = ev.evento_id
        group by id_cnpj_sucedida
    ),

    contato as (
        select id_cnpj, array_agg(struct(ddd, telefone)) as telefone
        from
            (
                select
                    id_cnpj,
                    case
                        when
                            contato_ddd_1 not in (
                                '11',
                                '12',
                                '13',
                                '14',
                                '15',
                                '16',
                                '17',
                                '18',
                                '19',
                                '21',
                                '22',
                                '24',
                                '27',
                                '28',
                                '31',
                                '32',
                                '33',
                                '34',
                                '35',
                                '37',
                                '38',
                                '41',
                                '42',
                                '43',
                                '44',
                                '45',
                                '46',
                                '47',
                                '48',
                                '49',
                                '51',
                                '53',
                                '54',
                                '55',
                                '61',
                                '62',
                                '63',
                                '64',
                                '65',
                                '66',
                                '67',
                                '68',
                                '69',
                                '71',
                                '73',
                                '74',
                                '75',
                                '77',
                                '79',
                                '81',
                                '82',
                                '83',
                                '84',
                                '85',
                                '86',
                                '87',
                                '88',
                                '89',
                                '91',
                                '92',
                                '93',
                                '94',
                                '95',
                                '96',
                                '97',
                                '98',
                                '99'
                            )
                        then null
                        else contato_ddd_1
                    end as ddd,
                    {{ padronize_telefone("contato_telefone_1") }} as telefone
                from cnpj_renomeada
                where contato_telefone_1 is not null
                union all
                select
                    id_cnpj,
                    case
                        when
                            contato_ddd_2 not in (
                                '11',
                                '12',
                                '13',
                                '14',
                                '15',
                                '16',
                                '17',
                                '18',
                                '19',
                                '21',
                                '22',
                                '24',
                                '27',
                                '28',
                                '31',
                                '32',
                                '33',
                                '34',
                                '35',
                                '37',
                                '38',
                                '41',
                                '42',
                                '43',
                                '44',
                                '45',
                                '46',
                                '47',
                                '48',
                                '49',
                                '51',
                                '53',
                                '54',
                                '55',
                                '61',
                                '62',
                                '63',
                                '64',
                                '65',
                                '66',
                                '67',
                                '68',
                                '69',
                                '71',
                                '73',
                                '74',
                                '75',
                                '77',
                                '79',
                                '81',
                                '82',
                                '83',
                                '84',
                                '85',
                                '86',
                                '87',
                                '88',
                                '89',
                                '91',
                                '92',
                                '93',
                                '94',
                                '95',
                                '96',
                                '97',
                                '98',
                                '99'
                            )
                        then null
                        else contato_ddd_2
                    end as ddd,
                    {{ padronize_telefone("contato_telefone_2") }} as telefone
                from cnpj_renomeada
                where contato_telefone_2 is not null
            )
        group by id_cnpj
    ),

    cnpj_base_intermediaria as (
        select
            -- Primary key
            case
                when t.id_cnpj is null then t.id_cnpj_sucedida else t.id_cnpj
            end as id_cnpj,

            -- Foreign keys
            t.id_municipio,
            t.id_pais,
            t.id_natureza_juridica,
            t.id_qualificacao_responsavel,
            t.id_porte,
            t.id_indicador_matriz,
            t.id_tipo_orgao_registro,
            t.id_motivo_situacao,
            t.id_situacao_cadastral,

            -- Business data
            t.razao_social,
            t.nome_fantasia,
            t.capital_social,
            t.cnae_fiscal,
            t.cnae_secundarias,
            t.nire,

            -- Dates
            t.data_inicio_atividade,
            t.data_situacao_cadastral,
            t.data_situacao_especial,
            t.data_inclusao_responsavel,

            -- Status and demographics
            t.situacao_especial,
            t.id_ente_federativo,
            case
                when id_ente_federativo = 'BR'
                then 'União'
                when regexp_contains(id_ente_federativo, r'^[0-9]+$')
                then 'Município'
                when
                    upper(id_ente_federativo)
                    in (select upper(id_ente_federativo) from sigla_uf_bd)
                then 'Estado'
                else null
            end as ente_federativo_tipo,

            -- Contact
            tel.telefone as contato_telefone,
            t.contato_email,

            -- Address
            t.endereco_cep,
            t.endereco_uf,
            t.endereco_bairro,
            t.endereco_tipo_logradouro,
            t.endereco_logradouro,
            t.endereco_numero,
            t.endereco_complemento,
            t.endereco_cidade_exterior_nome,

            -- Accountant Information
            t.contador_pf_tipo_crc,
            t.contador_pj_id,
            t.contador_pf_classificacao_crc,
            t.contador_pf_sequencial_crc,
            t.contador_pf_id,
            t.contador_pj_tipo_crc,
            t.contador_pj_classificacao_crc,
            t.contador_pj_uf_crc,
            t.contador_pf_uf_crc,
            t.contador_pj_sequencial_crc,

            -- Responsible Person
            t.responsavel_cpf,

            -- Business arrays
            actb.tipos_unidade,
            actb.formas_atuacao,
            array_length(soc.socios) as socios_quantidade,
            soc.socios,
            suc.sucessoes,

            -- Metadata
            t.timestamp,
            t.language,
            t.version,

            -- Outros
            t.airbyte,

            -- Joins
            md.municipio_nome as endereco_municipio_nome,
            sc.descricao as situacao_cadastral_descricao,
            ms.descricao as motivo_situacao_descricao,
            org.descricao as tipo_orgao_registro_descricao,
            nj.descricao as natureza_juridica_descricao,
            pe.descricao as porte_descricao,
            im.descricao as indicador_matriz_descricao,
            qr.descricao as qualificacao_responsavel_descricao,

            cast(t.id_cnpj as int64) as cnpj_particao

        from cnpj_renomeada t
        left join
            contato as tel
            on t.id_cnpj = tel.id_cnpj


        left join socios_tb as soc on t.id_cnpj = soc.id_cnpj
        left join
            sucessoes_tb as suc
            on t.id_cnpj_sucedida = suc.id_cnpj_sucedida

        left join array_convert_tb as actb on t.id_cnpj = actb.id_cnpj
        left join
            municipio_bd as md
            on cast(t.id_municipio as int64) = cast(md.id_municipio_rf as int64)
        left join
            (
                select id as id_situacao_cadastral, descricao
                from dominio
                where column = 'situacao_cadastral'
            ) sc
            on t.id_situacao_cadastral = sc.id_situacao_cadastral
        left join
            (
                select id as id_motivo_situacao, descricao
                from dominio
                where column = 'motivo_situacao_cadastral'
            )
            ms on t.id_motivo_situacao = ms.id_motivo_situacao
        left join
            (
                select id as id_tipo_orgao_registro, descricao
                from dominio
                where column = 'tipo_orgao_registro'
            )
            org on t.id_tipo_orgao_registro = org.id_tipo_orgao_registro
        left join
            (
                select id as id_natureza_juridica, descricao
                from dominio
                where column = 'natureza_juridica'
            )
            nj on t.id_natureza_juridica = nj.id_natureza_juridica
        left join
            (
                select id as id_porte_empresa, descricao
                from dominio
                where column = 'porte_empresa'
            )
            pe on t.id_porte = pe.id_porte_empresa
        left join
            (
                select id as id_indicador_matriz, descricao
                from dominio
                where column = 'indicador_matriz'
            )
            im on t.id_indicador_matriz = im.id_indicador_matriz
        left join
            (
                select id as id_qualificacao_responsavel, descricao
                from dominio
                where column = 'qualificacao_responsavel'
            )
            qr on t.id_qualificacao_responsavel = qr.id_qualificacao_responsavel
    ),

    cnpj_base_padronizada as (
        select
            -- Primary key
            t.id_cnpj,

            -- Foreign keys
            t.id_municipio,
            t.id_pais,
            t.id_natureza_juridica,
            t.id_qualificacao_responsavel,
            t.id_porte,
            t.id_indicador_matriz,
            t.id_tipo_orgao_registro,
            t.id_motivo_situacao,
            t.id_situacao_cadastral,

            -- Business data
            razao_social,
            nome_fantasia,  -- Padronizado
            t.capital_social,
            t.cnae_fiscal,
            t.cnae_secundarias,
            t.nire,
            -- Dates
            t.data_inicio_atividade,
            t.data_situacao_cadastral,
            t.data_situacao_especial,
            t.data_inclusao_responsavel,

            -- Status and demographics
            {{ proper_br("situacao_especial") }} as situacao_especial,  -- Padronizado
            t.id_ente_federativo,
            {{ proper_br("ente_federativo_tipo") }} as ente_federativo_tipo,

            -- Contact
            t.contato_telefone,
            t.contato_email,

            -- Address
            t.endereco_cep,
            t.endereco_uf,
            {{ proper_br("endereco_bairro") }} as endereco_bairro,  -- Padronizado
            {{ proper_br("endereco_tipo_logradouro") }} as endereco_tipo_logradouro,  -- Padronizado
            {{ proper_br("endereco_logradouro") }} as endereco_logradouro,  -- Padronizado
            t.endereco_numero,
            {{ proper_br("endereco_complemento") }} as endereco_complemento,  -- Padronizado
            t.endereco_cidade_exterior_nome,
            t.endereco_municipio_nome,

            -- Accountant Information
            t.contador_pf_tipo_crc,
            t.contador_pj_id,
            t.contador_pf_classificacao_crc,
            t.contador_pf_sequencial_crc,
            t.contador_pf_id,
            t.contador_pj_tipo_crc,
            t.contador_pj_classificacao_crc,
            t.contador_pj_uf_crc,
            t.contador_pf_uf_crc,
            t.contador_pj_sequencial_crc,

            -- Responsible Person
            t.responsavel_cpf,

            -- Business arrays
            t.tipos_unidade,
            t.formas_atuacao,
            t.socios_quantidade,
            t.socios,
            t.sucessoes,
            -- Metadata
            t.timestamp,
            t.language,

            -- Outros
            t.airbyte,
            
            -- descricoes
            {{ proper_br("tipo_orgao_registro_descricao") }}
            as tipo_orgao_registro_descricao,
            {{ proper_br("motivo_situacao_descricao") }} as motivo_situacao_descricao,
            {{ proper_br("situacao_cadastral_descricao") }}
            as situacao_cadastral_descricao,
            {{ proper_br("natureza_juridica_descricao") }}
            as natureza_juridica_descricao,
            {{ proper_br("porte_descricao") }} as porte_descricao,
            {{ proper_br("indicador_matriz_descricao") }} as indicador_matriz_descricao,
            {{ proper_br("qualificacao_responsavel_descricao") }}
            as responsavel_qualificacao_descricao,
            -- Partition and Rank
            t.cnpj_particao
        from cnpj_base_intermediaria t
    ),

    cnpj_base_deduplicada as (
        select *
        from cnpj_base_padronizada
        qualify
            row_number() over (
                partition by id_cnpj order by data_situacao_cadastral desc
            )
            = 1
    ),

    final as (
        select
            -- Primary key
            id_cnpj as cnpj,

            -- Business data
            razao_social,
            nome_fantasia,
            capital_social,
            cnae_fiscal,
            cnae_secundarias,
            nire,

            struct(
                id_natureza_juridica as id, natureza_juridica_descricao as descricao
            ) as natureza_juridica,

            struct(id_porte as id, porte_descricao as descricao) as porte,

            struct(
                id_indicador_matriz as id, indicador_matriz_descricao as descricao
            ) as matriz_filial,

            struct(
                id_tipo_orgao_registro as id, tipo_orgao_registro_descricao as descricao
            ) as orgao_registro,

            -- Dates
            data_inicio_atividade as inicio_atividade_data,

            -- Status and demographics
            struct(
                id_situacao_cadastral as id,
                situacao_cadastral_descricao as descricao,
                data_situacao_cadastral as data,
                id_motivo_situacao as motivo_id,
                motivo_situacao_descricao as motivo_descricao
            ) as situacao_cadastral,
            struct(
                situacao_especial as descricao, data_situacao_especial as data
            ) as situacao_especial,
            struct(
                id_ente_federativo as id, ente_federativo_tipo as tipo
            ) as ente_federativo,

            -- Contact information grouped in struct
            struct(contato_telefone as telefone, contato_email as email) as contato,

            -- Address information grouped in struct
            struct(
                endereco_cep as cep,
                id_pais,
                endereco_uf as uf,
                id_municipio,
                endereco_municipio_nome as municipio_nome,
                endereco_cidade_exterior_nome as municipio_exterior_nome,
                endereco_bairro as bairro,
                endereco_tipo_logradouro as tipo_logradouro,
                endereco_logradouro as logradouro,
                endereco_numero as numero,
                endereco_complemento as complemento

            ) as endereco,

            -- Accountant information grouped in struct
            struct(
                struct(
                    contador_pf_tipo_crc as tipo_crc,
                    contador_pf_classificacao_crc as classificacao_crc,
                    contador_pf_sequencial_crc as sequencial_crc,
                    contador_pf_id as id
                ) as pf,
                struct(
                    contador_pj_id as id,
                    contador_pj_tipo_crc as tipo_crc,
                    contador_pj_classificacao_crc as classificacao_crc,
                    contador_pj_sequencial_crc as sequencial_crc
                ) as pj
            ) as contador,

            -- Responsible Person
            struct(
                responsavel_cpf as cpf,
                id_qualificacao_responsavel as qualificacao_id,
                responsavel_qualificacao_descricao as qualificacao_descricao,
                data_inclusao_responsavel as inclusao_data
            ) as responsavel,

            -- Business arrays
            tipos_unidade,
            formas_atuacao,
            socios_quantidade,
            socios,
            sucessoes,

            -- Metadata
            timestamp,
            language,

            -- Outros
            airbyte,

            -- Partition
            cnpj_particao

        from cnpj_base_deduplicada
    )

select *
from final
