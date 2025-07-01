{{
    config(
      schema="brutos_cadastro_comercio_ambulantes",
      alias="autorizacao_auxiliares",
      materialized="table",
      tags=["raw", "cadastro_comercio_ambulantes"],
      description="Dados do Auxiliar do Titular da Autorização"
    )
}}

select 
    safe_cast(idAutorizacao as integer) as id_autorizacao_titular,
    safe_cast(inscricaoMunicipal as string) as numero_inscricao_municipal,
    safe_cast(nome as string) as nome_titular_autorizacao,
    safe_cast(cpf as string) as cpf_auxiliar,
    safe_cast(tipo_pessoa as string) as tipo_pessoa,
    safe_cast(inicioVigenciaAuxiliar as datetime) as data_inicio_vigencia_auxiliar,
   
from {{ source('brutos_cadastro_comercio_ambulantes_staging', 'vw_autorizacao_auxiliares_datalake') }}  