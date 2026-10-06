{{
    config(
        schema="app_eleicoes",
        alias="dim_perfil_eleitorado_zona",
        materialized="table",
        cluster_by=["ano", "id_zona"],
        tags=["app_eleicoes"],
    )
}}

-- perfil do eleitorado por zona, com os códigos do TSE decodificados.
-- a fonte também separa por situação da biometria, que aqui é somada
with
    perfil as (
        select
            ano,
            {{ id_zona("id_municipio", "zona") }} as id_zona,
            cast(genero as string) as genero_codigo,
            cast(grupo_idade as string) as grupo_idade_codigo,
            cast(instrucao as string) as instrucao_codigo,
            cast(estado_civil as string) as estado_civil_codigo,
            sum(eleitores) as eleitores,
            sum(eleitores_biometria) as eleitores_biometria,
            sum(eleitores_deficiencia) as eleitores_deficiencia
        from {{ source("basedosdados_br_tse_eleicoes", "perfil_eleitorado_municipio_zona") }}
        where sigla_uf = 'RJ' and ano in (2022, 2024, 2026)
        group by
            ano,
            id_zona,
            genero_codigo,
            grupo_idade_codigo,
            instrucao_codigo,
            estado_civil_codigo
    )

select
    ano,
    id_zona,
    genero_codigo,
    -- no TSE, 2 é masculino e 4 é feminino
    case
        genero_codigo
        when '0'
        then 'Não informado'
        when '2'
        then 'Masculino'
        when '4'
        then 'Feminino'
    end as genero,
    grupo_idade_codigo,
    -- 1600 a 2000 são idades individuais; depois vêm faixas de 5 anos (por exemplo 2529)
    case
        when grupo_idade_codigo = '-3'
        then 'Inválido'
        when grupo_idade_codigo = '9999'
        then '100 anos ou mais'
        when grupo_idade_codigo between '1600' and '2000'
        then concat(substr(grupo_idade_codigo, 1, 2), ' anos')
        else
            concat(substr(grupo_idade_codigo, 1, 2), ' a ', substr(grupo_idade_codigo, 3, 2), ' anos')
    end as faixa_etaria,
    instrucao_codigo,
    case
        instrucao_codigo
        when '0'
        then 'Não informado'
        when '1'
        then 'Analfabeto'
        when '2'
        then 'Lê e escreve'
        when '3'
        then 'Fundamental incompleto'
        when '4'
        then 'Fundamental completo'
        when '5'
        then 'Médio incompleto'
        when '6'
        then 'Médio completo'
        when '7'
        then 'Superior incompleto'
        when '8'
        then 'Superior completo'
    end as instrucao,
    estado_civil_codigo,
    case
        estado_civil_codigo
        when '-3'
        then 'Inválido'
        when '0'
        then 'Não informado'
        when '1'
        then 'Solteiro'
        when '3'
        then 'Casado'
        when '5'
        then 'Viúvo'
        when '7'
        then 'Separado judicialmente'
        when '9'
        then 'Divorciado'
    end as estado_civil,
    eleitores,
    eleitores_biometria,
    eleitores_deficiencia
from perfil
