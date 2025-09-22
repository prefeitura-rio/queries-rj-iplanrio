{{
    config(
        schema="brutos_divida_ativa",
        alias="imovel",
        materialized="table",
        tags=["raw", "divida_ativa", "divida", "ativa", "imóvel"],
        description="Tabela que os dados de imóveis que estejam vinculados às certidões de dívida ativa (CDA), nos casos de dívida de IPTU ou Mais Valia."
    )
}}

/*
with tipologia as (
    select codTipologia, descTipologia from {{ ref('raw_divida_ativa_tipologia_imovel') }}
),
utilizacao_imovel as (
    select CodUtilizacao, Descricao from {{ ref('raw_divida_ativa_utilizacao_imovel') }}
)
*/

select safe_cast(a.codInscricaoImobiliaria as int64) as inscricao_imobiliaria,
    struct(
        safe_cast(codLogradouro as int64) as codigo_logradouro,
        safe_cast(descEndereco as string) as nome_logradouro,
        safe_cast(numEndereco as string) as numero_porta,
        safe_cast(descComplemento as string) as complemento_endereco,
        safe_cast(nomBairro as string) as bairro,
        safe_cast(numCep as string) as cep
    ) as endereco,
    struct(
        safe_cast(a.codTipologia as int64) as id_tipologia_imovel,
        safe_cast(ifnull(b.descTipologia, 'NÃO IDENTIFICADO') as string) as nome_tipologia_imovel
    ) as tipologia_imovel,
    struct(
        safe_cast(a.CodUtilizacao as int64) as id_utilizacao_imovel,
        safe_cast(ifnull(c.Descricao, 'NÃO IDENTIFICADO') as string) as nome_utilizacao_imovel
    ) as utilizacao_imovel,
    a._airbyte_extracted_at as loaded_at,
    current_timestamp() as transformed_at
from {{ source('brutos_divida_ativa_staging', 'InscricaoImobiliaria') }} a
left join {{ source('brutos_divida_ativa_staging', 'TipologiaImovel') }} b on b.codTipologia = a.codTipologia
left join {{ source('brutos_divida_ativa_staging', 'Utilizacao_Imovel') }} c on c.CodUtilizacao = a.CodUtilizacao

/*
from {{ source('brutos_divida_ativa_staging', 'InscricaoImobiliaria') }} imovel
left join {{ ref('raw_divida_ativa_tipologia_imovel') }} tipologia on tipologia.codTipologia = imovel.codTipologia
left join {{ ref('raw_divida_ativa_utilizacao_imovel') }} utilizacao on utilizacao.CodUtilizacao = imovel.CodUtilizacao
*/
