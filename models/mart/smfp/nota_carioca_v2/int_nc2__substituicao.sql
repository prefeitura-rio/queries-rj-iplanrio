{{ config(cluster_by=["chave_acesso"]) }}

-- R-09 (só a marcação da nota substituída): a nota que traz CHAVE_ACESSO_SUBSTITUIDA
-- cancela a nota substituída, desde que ela exista (RPE_NOTASNACIONAIS.pkb Regra 2).
-- Correção catalogada B-06/R-09: DATA_CANCELAMENTO é a DATA_VALIDACAO da substituta
-- (PL/SQL); o VB gravava DPS.DATA_EMISSAO, que vai em data_emissao_substituta só para
-- a paridade reconhecer. Duas substitutas para a mesma nota (a
-- UN_CHAVE_ACESSO_SUBSTITUIDA do Oracle barra a segunda): vale a de menor NSU. Não
-- gera crédito nem cancela guia: isso fica no Oracle (fase 1).
with
    substitutas as (
        select
            chave_acesso_substituida as chave_acesso,
            chave_acesso as chave_acesso_substituta,
            data_validacao as data_cancelamento,
            data_emissao as data_emissao_substituta,
            nsu as nsu_substituta
        from {{ ref("int_nc2__dedup") }}
        where chave_acesso_substituida is not null
        qualify
            row_number() over (partition by chave_acesso_substituida order by nsu asc)
            = 1
    )

select s.*
from substitutas as s
inner join {{ ref("int_nc2__dedup") }} as alvo on alvo.chave_acesso = s.chave_acesso
