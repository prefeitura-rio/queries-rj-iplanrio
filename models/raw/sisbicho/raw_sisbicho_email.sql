{{
    config(   
      alias="email",   
      tags=["raw", "sisbicho"],
      description="Tabela de e-mails do sistema"
    )
}}

select
    safe_cast(IDEMail as integer) as id_email,
    safe_cast(Identificacao as string) as identificacao_codigo,
    safe_cast(Texto as string) as texto_conteudo,
    safe_cast(Assunto as string) as assunto_nome,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at
FROM {{source('brutos_sisbicho_staging', 'EMail') }} 