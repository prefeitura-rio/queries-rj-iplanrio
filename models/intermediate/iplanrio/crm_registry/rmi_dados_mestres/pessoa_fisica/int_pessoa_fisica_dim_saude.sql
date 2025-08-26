-- Consolida informações de saúde de pessoa física a partir de múltiplas fontes do
-- município do Rio de Janeiro
-- Este modelo gera um struct de informações de saúde por CPF, unificando dados de saúde
{{
    config(
        alias="dim_saude",
        schema="intermediario_dados_mestres",
        materialized=("table" if target.name == "dev" else "ephemeral"),
        tags=["daily"],
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

with
    -- sources
    all_cpf as (select cpf, cpf_particao from {{ ref("int_pessoa_fisica_all_cpf") }}),

    source_sms as (
        select b.*
        from all_cpf a
        inner join {{ source("rj-sms", "paciente") }} b using (cpf)
    ),

    dim_clinica_familia as (
        select * from {{ source("rj-sms-dados-mestres", "estabelecimento") }}
    ),

    dim_unidades as (
        select * from {{ source("brutos_plataforma_subpav", "unidades")}}
    ),

    -- Equipe de saúde familiar (deduplicated)
    equipe_saude_familia_raw as (
        select cpf, equipe_saude_familia[offset(0)] as equipe_saude_familia
        from source_sms
        where array_length(equipe_saude_familia) > 0
    ),
    
    equipe_saude_familia_struct as (
        select * except(row_num)
        from (
            select *,
                row_number() over (
                    partition by cpf 
                    order by equipe_saude_familia.id_ine desc nulls last
                ) as row_num
            from equipe_saude_familia_raw
        )
        where row_num = 1
    ),

    -- Clínica de saúde familiar (deduplicated)
    clinica_familia_raw as (
        select
            cpf,
            eqp.equipe_saude_familia.clinica_familia.id_cnes,
            eqp.equipe_saude_familia.clinica_familia.nome,
            eqp.equipe_saude_familia.clinica_familia.telefone,
            dcf.email,
            {{ proper_br("concat(
                dcf.endereco_logradouro,
                ' ',
                dcf.endereco_numero,
                ', ',
                dcf.endereco_bairro
            )") }} as endereco,
            CONCAT(funcionamento_dia_util_inicio, ":00 às ",funcionamento_dia_util_fim, ":00") as horario_atendimento_dia_util,
            CONCAT(funcionamento_sabado_inicio, ":00 às ",funcionamento_sabado_fim, ":00") as horario_atendimento_sabado
        from equipe_saude_familia_struct as eqp
        left join
            dim_clinica_familia as dcf
            on eqp.equipe_saude_familia.clinica_familia.id_cnes
            = dcf.id_cnes
        left join dim_unidades as du on eqp.equipe_saude_familia.clinica_familia.id_cnes = du.id_cnes
    ),
    
    clinica_familia_struct as (
        select * except(row_num)
        from (
            select *,
                row_number() over (
                    partition by cpf 
                    order by id_cnes desc nulls last
                ) as row_num
            from clinica_familia_raw
        )
        where row_num = 1
    ),

    -- Dimensão de saúde
    dim_saude as (
        select
            all_cpf.cpf,
            struct(
            struct(
                if(clinica_familia_struct.id_cnes is not null, true, false) as indicador,
                clinica_familia_struct.id_cnes,
                clinica_familia_struct.nome,
                clinica_familia_struct.telefone,
                clinica_familia_struct.email,
                clinica_familia_struct.endereco,
                clinica_familia_struct.horario_atendimento_dia_util,
                clinica_familia_struct.horario_atendimento_sabado
            ) as clinica_familia,
            struct(
                if(equipe_saude_familia is not null, true, false) as indicador,
                equipe_saude_familia.id_ine,
                equipe_saude_familia.nome,
                equipe_saude_familia.telefone,
                equipe_saude_familia.medicos,
                equipe_saude_familia.enfermeiros
            ) as equipe_saude_familia
            ) as saude,
            cast(all_cpf.cpf as int64) as cpf_particao
        from all_cpf
        left join equipe_saude_familia_struct using (cpf)
        left join clinica_familia_struct using (cpf)
    )

select *
from dim_saude
