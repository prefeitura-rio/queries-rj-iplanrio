{{
    config(
      alias="credenciada_usuario",
      project=("rj-iplanrio" if target.name == "prod" else "rj-iplanrio-dev") , 
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de usuários das credenciadas"
    )
}}
select
    safe_cast(IDCredenciadaUsuario as integer) as id_credenciada_usuario,
    safe_cast(IDCredenciada as integer) as id_credenciada,
    safe_cast(USR_CODIGO as integer) as usuario_codigo,
    safe_cast(LOGIN_REGISTRO as string) as usuario_login,
    safe_cast(DATA_REGISTRO as datetime) as registro_datahora,
    safe_cast(ASSOCIADO as string) as associado_indicador,
    safe_cast(Gestor as string) as gestor_indicador,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at
FROM {{ source('brutos_sisbicho_staging', 'CredenciadaUsuario') }} 