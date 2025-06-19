{{
    config(
      schema="brutos_sisbicho",
      alias="email",
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de e-mails do sistema"
    )
}}

SELECT 
    SAFE_cast(IDEMail as integer) as id_email,
    SAFE_cast(Identificacao as string) as identificacao_codigo,
    SAFE_cast(Texto as string) as texto_conteudo,
    SAFE_cast(Assunto as string) as assunto_nome
FROM {{ source('brutos_sisbicho_staging', 'EMail') }} 