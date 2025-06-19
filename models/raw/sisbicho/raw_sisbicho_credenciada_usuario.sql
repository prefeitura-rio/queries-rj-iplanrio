{{
    config(
      schema="brutos_sisbicho",
      alias="credenciada_usuario",
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de usuários das credenciadas"
    )
}}
SELECT 
    SAFE_cast(IDCredenciadaUsuario as integer) as id_credenciada_usuario,
    SAFE_cast(IDCredenciada as integer) as id_credenciada,
    SAFE_cast(USR_CODIGO as integer) as usuario_codigo,
    SAFE_cast(LOGIN_REGISTRO as string) as usuario_login,
    SAFE_cast(DATA_REGISTRO as datetime) as registro_datahora,
    SAFE_cast(ASSOCIADO as string) as associado_indicador,
    SAFE_cast(Gestor as string) as gestor_indicador
FROM {{ source('brutos_sisbicho_staging', 'CredenciadaUsuario') }} 