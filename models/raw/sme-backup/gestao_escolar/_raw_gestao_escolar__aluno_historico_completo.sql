{{ config(alias='aluno_historico_completo', schema='brutos_gestao_escolar') }}

with source as (
    select * from {{ source('sme_brutos_gestao_escolar_staging_prefect', 'VW_BI_Aluno_Todos_Os_Anos') }}
),

renamed as (
    select
        {{ adapter.quote("_prefect_extracted_at") }} AS loaded_at,
        {{ adapter.quote('Ano') }},
        {{ adapter.quote('Matricula') }},
        {{ adapter.quote('Nome') }},
        {{ adapter.quote('Sexo') }},
        {{ adapter.quote('Endereco') }},
        {{ adapter.quote('Bairro') }},
        {{ adapter.quote('CEP') }},
        {{ adapter.quote('Filiacao_1') }},
        {{ adapter.quote('Filiacao_2') }},
        {{ adapter.quote('Mora_com_Filiacao') }},
        {{ adapter.quote('CPF') }},
        {{ adapter.quote('NIS_Aluno') }},
        {{ adapter.quote('NIS_Resp') }},
        {{ adapter.quote('Raca_Cor') }},
        {{ adapter.quote('Cod_def') }},
        {{ adapter.quote('Deficiencia') }},
        {{ adapter.quote('Tipo_Transporte') }},
        {{ adapter.quote('Bolsa_Familia') }},
        {{ adapter.quote('CFC') }},
        {{ adapter.quote('Territorios_Sociais') }},
        {{ adapter.quote('Clube_Escolar') }},
        {{ adapter.quote('Nucleo_Artes') }},
        {{ adapter.quote('Mais_Educacao') }},
        {{ adapter.quote('DataNascimento') }},
        {{ adapter.quote('Idade_Atual') }},
        {{ adapter.quote('Idade_3112') }},
        {{ adapter.quote('Grupamento') }},
        {{ adapter.quote('Turma') }},
        {{ adapter.quote('UP_Aval') }},
        {{ adapter.quote('telefone') }},
        {{ adapter.quote('telefone_casa') }},
        {{ adapter.quote('telefone_recado') }},
        {{ adapter.quote('telefone_trab') }},
        {{ adapter.quote('celular_1') }},
        {{ adapter.quote('celular_2') }},
        {{ adapter.quote('Situacao') }},
        {{ adapter.quote('Cod_Ult_Mov') }},
        {{ adapter.quote('Ult_Movimentacao') }},
        {{ adapter.quote('Tot_Aluno') }},
        {{ adapter.quote('alu_id') }},
        {{ adapter.quote('tur_id') }}
    from source
)


select * from renamed
