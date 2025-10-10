{{
    config(
        schema="brutos_divida_ativa",
        alias="mensagem_protesto",
        materialized="table",
        tags=["raw", "divida_ativa", "protestos", "cda"],
        description="Tabela que contém os tipos de situação dos protestos atualizados pelos cartórios."
    )
}}

select safe_cast(idProtestosMensagens as int64) as id_mensagem_protesto,
    safe_cast(codigo as string) as codigo_mensagem_protesto,
    safe_cast(descricao as string) as descricao_mensagem_protesto,
    safe_cast(tipoServico as string) as tipo_servico,
    _airbyte_extracted_at as loaded_at,
    current_timestamp() as transformed_at
from {{ source('brutos_divida_ativa_staging', 'ProtestosMensagens') }}