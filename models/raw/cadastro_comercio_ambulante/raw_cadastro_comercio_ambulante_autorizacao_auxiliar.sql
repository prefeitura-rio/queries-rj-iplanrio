{{
    config(
      schema="cadastro_comercio_ambulante",
      alias="autorizacao_auxiliares",
      materialized="table",
      tags=["raw", "cadastro_comercio_ambulante"],
      description="Dados do Auxiliar do Titular da Autorização"
    )
}}

SELECT 
    SAFE_CAST(idAutorizacao as integer) as id_autoriz_titular,
    SAFE_CAST(inscricaoMunicipal as String) as num_inscricao_municipal,
    SAFE_CAST(nome as String) as nome_titular_autoriz,
    SAFE_CAST(cpf as String) as cpf_auxiliar,
    SAFE_CAST(tipo_pessoa as String) as tipo_pessoa,
    SAFE_CAST(inicioVigenciaAuxiliar as datetime) as data_inicio_vigencia_auxiliar
FROM {{ source('brutos_cadastro_comercio_ambulante_staging', 'autorizacao_auxiliares') }}  