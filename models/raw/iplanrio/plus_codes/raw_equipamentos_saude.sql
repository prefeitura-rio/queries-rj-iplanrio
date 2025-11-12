{{
    config(
        alias="equipamentos_saude",
        schema="plus_codes",
        materialized="table",
    )
}}

{# CREATE OR REPLACE TABLE `rj-iplanrio.plus_codes.equipamentos_saude` AS ( #}
with
    tipo_mapping as (
        select tipo, tipo_sms_equivalente
        from unnest([
            -- Direct matches
            struct('CENTRAL DE ABASTECIMENTO' as tipo, 'CENTRAL DE ABASTECIMENTO' as tipo_sms_equivalente),
            struct('CENTRAL DE GESTAO EM SAUDE', 'CENTRAL DE GESTAO EM SAUDE'),
            struct('CENTRAL DE REGULACAO DO ACESSO', 'CENTRAL DE REGULACAO DO ACESSO'),
            struct('CENTRAL DE REGULACAO MEDICA DAS URGENCIAS', 'CENTRAL DE REGULACAO DO ACESSO'),
            struct('CENTRO DE ATENCAO PSICOSSOCIAL', 'CENTRO DE ATENCAO PSICOSSOCIAL'),
            struct('CENTRO DE IMUNIZACAO', 'CENTRO DE IMUNIZACAO'),
            struct('CENTRO DE SAUDE/UNIDADE BASICA', 'CENTRO MUNICIPAL DE SAUDE'),
            struct('CLINICA/CENTRO DE ESPECIALIDADE', 'CLINICA/CENTRO DE ESPECIALIDADE'),
            struct('LABORATORIO DE SAUDE PUBLICA', 'LABORATORIO DE SAUDE PUBLICA'),
            struct('POLICLINICA', 'POLICLINICA'),
            struct('POLO DE PREVENCAO DE DOENCAS E AGRAVOS E PROMOCAO DA SAUDE', 'POLO DE PREVENCAO DE DOENCAS E AGRAVOS E PROMOCAO DA SAUDE'),
            struct('UNIDADE DE APOIO DIAGNOSE E TERAPIA (SADT ISOLADO)', 'UNIDADE DE APOIO DIAGNOSE E TERAPIA'),
            struct('UNIDADE DE VIGILANCIA EM SAUDE', 'UNIDADE DE VIGILANCIA EM SAUDE'),
            -- Hospital-related
            struct('HOSPITAL ESPECIALIZADO', 'HOSPITAL'),
            struct('HOSPITAL GERAL', 'HOSPITAL'),
            struct('HOSPITAL/DIA - ISOLADO', 'HOSPITAL'),
            -- Urgency/Emergency
            struct('PRONTO ATENDIMENTO', 'UNIDADE DE PRONTO ATENDIMENTO'),
            struct('PRONTO SOCORRO GERAL', 'UNIDADE DE PRONTO ATENDIMENTO'),
            struct('UNIDADE MOVEL DE NIVEL PRE-HOSPITALAR NA AREA DE URGENCIA', 'UNIDADE DE PRONTO ATENDIMENTO'),
            struct('UNIDADE MOVEL TERRESTRE', 'UNIDADE DE PRONTO ATENDIMENTO'),
            -- Maternity
            struct('CENTRO DE PARTO NORMAL - ISOLADO', 'MATERNIDADE'),
            -- Others
            struct('CENTRAL DE NOTIFICACAO,CAPTACAO E DISTRIB DE ORGAOS ESTADUAL', 'OUTROS'),
            struct('CONSULTORIO ISOLADO', 'OUTROS'),
            struct('FARMACIA', 'OUTROS'),
            struct('OFICINA ORTOPEDICA', 'OUTROS'),
            struct('TELESSAUDE', 'OUTROS'),
            struct('UNIDADE MISTA', 'OUTROS')
        ])
    ),

    tb as (
        select
            -- Pluscodes (computed and ordered as specified)
            coalesce(
                tools.encode_pluscode(t.endereco_latitude, t.endereco_longitude, 11), ''
            ) as plus11,

            -- Core identification
            t.id_cnes as id_equipamento,
            'SMS' as secretaria_responsavel,
            coalesce(
                nullif(trim(t.tipo_sms), ''),
                m.tipo_sms_equivalente,
                'OUTROS'
            ) as tipo_equipamento,
            t.nome_fantasia as nome_oficial,
            t.nome_limpo as nome_popular,

            -- More Pluscodes
            coalesce(
                tools.encode_pluscode(t.endereco_latitude, t.endereco_longitude, 10), ''
            ) as plus10,
            coalesce(
                tools.encode_pluscode(t.endereco_latitude, t.endereco_longitude, 8), ''
            ) as plus8,
            coalesce(
                tools.encode_pluscode(t.endereco_latitude, t.endereco_longitude, 6), ''
            ) as plus6,

            -- Location details
            t.endereco_latitude as latitude,
            t.endereco_longitude as longitude,
            st_geogpoint(t.endereco_longitude, t.endereco_latitude) as geometry,

            -- Structured address
            struct(
                upper(t.endereco_logradouro) as logradouro,
                t.endereco_numero as numero,
                upper(t.endereco_complemento) as complemento,
                upper(t.endereco_bairro) as bairro,  -- Renamed to avoid conflict with the new 'bairro' struct
                t.endereco_cep as cep
            ) as endereco,

            -- Structured bairro information from join
            struct(
                b.id_bairro as id_bairro,
                upper(b.nome) as bairro,
                upper(b.nome_regiao_planejamento) as regiao_planejamento,
                upper(b.nome_regiao_administrativa) as regiao_administrativa,
                upper(b.subprefeitura) as subprefeitura
            ) as bairro,

            -- Structured contact information
            struct(
                array(
                    select phone
                    from unnest(t.telefone) as phone
                    where phone is not null and trim(phone) != ''
                ) as telefones,
                t.email as email,
                cast(null as string) as site,  -- No direct mapping in source schema
                struct(
                    t.facebook as facebook,
                    t.instagram as instagram,
                    t.twitter as twitter
                ) as redes_social
            ) as contato,

            -- Status flags
            (lower(t.ativa) = 'sim') as ativo,
            case
                when coalesce(
                    nullif(trim(t.tipo_sms), ''),
                    m.tipo_sms_equivalente,
                    'OUTROS'
                ) = "HOSPITAL" and lower(t.aberto_sempre) = 'n' then false
                else (lower(t.ativa) = 'sim') 
            end as aberto_ao_publico,       case
                when
                    t.turno_atendimento
                    = 'ATENDIMENTO CONTINUO DE 24 HORAS/DIA (PLANTAO:INCLUI SABADOS, DOMINGOS E FERIADOS)'
                then  -- 24 hours / continuous
                    [
                        struct(
                            'Todos os dias' as dia,
                            time '00:00:00' as abre,
                            time '23:59:59' as fecha
                        )
                    ]
                when t.turno_atendimento = 'ATENDIMENTOS NOS TURNOS DA MANHA E A TARDE'
                then  -- Manhã e Tarde (assumed 8h-17h, Mon-Fri)
                    [
                        struct(
                            'Segunda a Sexta' as dia,
                            time '08:00:00' as abre,
                            time '17:00:00' as fecha
                        )
                    ]
                when
                    t.turno_atendimento
                    = 'ATENDIMENTO NOS TURNOS DA MANHA, TARDE E NOITE'
                then  -- Manhã, Tarde e Noite (assumed 8h-22h, Mon-Fri)
                    [
                        struct(
                            'Segunda a Sexta' as dia,
                            time '08:00:00' as abre,
                            time '22:00:00' as fecha
                        )
                    ]
                else  -- ATENDIMENTO COM TURNOS INTERMITENTES, return empty array
                    cast([] as array<struct<dia string, abre time, fecha time>>)
            end as horario_funcionamento,

            '{{ source("saude_dados_mestres", "estabelecimento") }}' as fonte,
            cast(null as date) as vigencia_inicio,
            cast(null as date) as vigencia_fim,

            -- Esfera (city, state, or federal level)
            t.esfera as esfera,

            -- Metadata as JSON (including all unused columns)
            to_json_string(
                struct(
                    t.id_unidade,
                    t.id_tipo_unidade,
                    t.area_programatica,
                    t.cnpj_mantenedora,
                    t.ativa as original_ativa,
                    t.tipo_sms_agrupado,
                    t.tipo,
                    t.tipo_sms_simplificado,
                    t.nome_acentuado,
                    t.nome_sigla,
                    t.nome_complemento,
                    t.responsavel_sms,
                    t.administracao,
                    t.prontuario_tem,
                    t.prontuario_versao,
                    t.prontuario_estoque_tem_dado,
                    t.prontuario_estoque_motivo_sem_dado,
                    t.prontuario_episodio_tem_dado,
                    t.aberto_sempre as original_aberto_sempre,
                    t.turno_atendimento as original_turno_atendimento,
                    -- t.diretor_clinico_cpf,
                    -- t.diretor_clinico_conselho,
                    t.data_atualizao_registro,
                    t.usuario_atualizador_registro,
                    t.data_particao,
                    t.data_carga,
                    t.data_snapshot
                )
            ) as metadata,

            -- Last update timestamp
            current_timestamp() as updated_at
        from {{ source("saude_dados_mestres", "estabelecimento") }} as t
        left join
            tipo_mapping as m
            on trim(t.tipo) = m.tipo
        left join
            {{ source("dados_mestres", "bairro") }} as b
            on st_contains(
                b.geometry, st_geogpoint(t.endereco_longitude, t.endereco_latitude)
            )
    )

select
    plus11,
    id_equipamento,
    trim(secretaria_responsavel) as secretaria_responsavel,
    trim(tipo_equipamento) as tipo_equipamento,
    nome_oficial,
    nome_popular,
    plus10,
    plus8,
    plus6,
    latitude,
    longitude,
    geometry,
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
from
    tb
    {# ) #}
    
