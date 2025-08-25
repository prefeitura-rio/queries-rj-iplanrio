{{
    config(
        schema="brutos_mais_valia",
        alias="contrapartida_consolidado",
        materialized="table",
        tags=["raw", "mais_valia", "mais", "valia", "contrapartida", "consolidado"],
        description="Tabela que espelha a view vwCTP_consolidado_ctp que consiste nos dados básicos dos cálculos de contrapartida."
    )
}}
with 
calculos_contrapartida as (
        select 
            safe_cast(origem as string) || "-" || safe_cast(codigo_calculo as string) as id_calculo,
            safe_cast(origem as int64) as origem,
            safe_cast(codigo_calculo as int64) as codigo_calculo,
            safe_cast(lc as string) as lei_contrapartida,
            safe_cast(assunto as string) as assunto,
            safe_cast(mais_valia as boolean) as mais_valia,
            safe_cast(mais_valera as boolean) as mais_valera,
            safe_cast(numero_processo as string) as numero_processo,
            safe_cast(ano_processo as int64) as ano,
            safe_cast(codigo_requerimento as int64) as codigo_requerimento,
            safe_cast(numero_requerimento as string) as numero_requerimento,
            safe_cast(ano_requerimento as int64) as ano_requerimento,
            safe_cast(codigo_logradouro as int64) as codigo_logradouro,
            safe_cast(codigo_logradouro_smf as int64) as codigo_logradouro_smf,
            safe_cast(tipo_logradouro as string) as tipo_logradouro,
            safe_cast(nobreza_logradouro as string) as nobreza_logradouro,
            safe_cast(preposicao_logradouro as string) as preposicao_logradouro,
            safe_cast(nome_logradouro as string) as nome_logradouro,
            safe_cast(numero_porta as string) as numero_porta,
            safe_cast(complemento_porta as string) as complemento_porta,
            safe_cast(unidade as string) as unidade,
            safe_cast(numero_pal as string) as numero_pal,
            safe_cast(lote as string) as lote,
            safe_cast(quadra as string) as quadra,
            safe_cast(codigo_bairro as int64) as codigo_bairro,
            safe_cast(bairro as string) as bairro,
            safe_cast(area_planejamento as int64) as area_planejamento,
            safe_cast(regiao_administrativa as int64) as regiao_administrativa,
            safe_cast(regiao_administrativa_romanos as string) as codigo_cepregiao_administrativa_romanos,
            safe_cast(requerente as string) as requerente,
            safe_cast(telefone as string) as telefone,
            safe_cast(celular as string) as celular,
            safe_cast(email as string) as email,
            safe_cast(cpf_cnpj as string) as cpf_cnpj,
            safe_cast(numero_laudo as string) as numero_laudo,
            safe_cast(valor as numeric) as valor,
            safe_cast(total_parcelas as int64) as quantidade_parcelas,
            safe_cast(total_parcelas_pagas as int64) as quantidade_parcelas_pagas,
            safe_cast(valor_parcelas_pagas as numeric) as valor_parcelas_pagas,
            safe_cast(total_parcelas_a_vencer as int64) as quantidade_parcelas_a_vencer,
            safe_cast(valor_parcelas_a_vencer as numeric) as valor_parcelas_a_vencer,
            safe_cast(total_parcelas_vencidas as int64) as quantidade_parcelas_vencidas,
            safe_cast(valor_parcelas_vencidas as numeric) as valor_parcelas_vencidas,
            safe_cast(codigo_status as int64) as codigo_status,
            CASE codigo_status
                WHEN 0 THEN 'Pagamento não iniciado'
                WHEN 1 THEN 'Contrapartida quitada'
                WHEN 2 THEN 'Pagamento em andamento'
                WHEN 3 THEN 'Com pendência de pagamento'
                WHEN 4 THEN 'Com forma de pagamento cancelada'
                ELSE 'Status desconhecido'
            END AS descricao_status,
            _airbyte_extracted_at as loaded_at,
            current_timestamp() as transformed_at
        from {{ source('brutos_mais_valia_staging', 'vwCTP_consolidado_ctp') }}
        where codigo_calculo is not null
),
dedup as (
select distinct * from calculos_contrapartida
qualify row_number() over (partition by id_calculo order by mais_valia desc, loaded_at desc) = 1
)

select * from dedup