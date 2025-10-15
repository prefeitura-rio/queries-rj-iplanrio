{{
    config(
        schema="brutos_divida_ativa",
        alias="utilizacao_imovel",
        materialized="table",
        tags=["raw", "divida_ativa", "utilização", "imovel"],
        description="Tabela que guarda as descrições dos possíveis tipos de utilização de imóveis vinculados às certidões de dívida ativa (CDA)."
    )
}}

select safe_cast(CodUtilizacao as int64) as id_utilizacao_imovel,
       safe_cast(Descricao as string) as nome_utilizacao_imovel,
       _airbyte_extracted_at as loaded_at,
       current_timestamp() as transformed_at
from {{ source('brutos_divida_ativa_staging', 'Utilizacao_Imovel') }}