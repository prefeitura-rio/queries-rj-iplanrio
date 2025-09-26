{{
    config(
        schema="brutos_divida_ativa",
        alias="pessoa",
        materialized="table",
        tags=["raw", "divida_ativa", "divida", "ativa", "pessoa"],
        description="Tabela que descreve os possíveis tipos de natureza de dívida das certidões de dívida ativa (CDA)."
    )
}}

select safe_cast(idpessoa as int64) as id_pessoa,
       safe_cast(Cpf_Cnpj as string) as cpf_cnpj,
       safe_cast(Nome as string) as nome,
       safe_cast(tipopessoa as int64) as tipo_pessoa,
       case safe_cast(tipopessoa as int64)
            when 1 then 'Pessoa Física'
            when 2 then 'Pessoa Jurídica'
            when null then 'Não Informado'
            else 'Não classificado'
       end as descricao_tipo_pessoa,
       _airbyte_extracted_at as loaded_at,
       current_timestamp() as transformed_at
from {{ source('brutos_divida_ativa_staging', 'VWPESSOA') }}