{{
    config(
        alias="parte_envolvida",
        description="Dados brutos de partes envolvidas do SICOP (VW_PARTE_ENVOLVIDA_DLK)"
    )
}}

select
    safe_cast (id_processo AS string) AS id_processo,
    safe_cast (id_sequencial_parte AS int64) AS id_sequencial_parte,
    safe_cast (nome_parte_envolvida AS string) AS nome_parte_envolvida,
    safe_cast (endereco_parte_envolvida AS string) AS endereco_parte_envolvida,
    safe_cast (numero_porta_parte_envolvida AS int64) AS numero_porta_parte_envolvida,
    safe_cast (complemento_parte_envolvida AS string) AS complemento_parte_envolvida,
    safe_cast (bairro_parte_envolvida AS string) AS bairro_parte_envolvida,
    safe_cast (cep_parte_envolvida AS int64) AS cep_parte_envolvida,
    safe_cast (telefone AS string) AS telefone,
    safe_cast (qualificacao AS string) AS qualificacao,
    safe_cast (data_procuracao AS datetime) AS data_procuracao,
    safe_cast (tipo_documento_parte_envolvida AS string) AS tipo_documento_parte_envolvida,
    safe_cast (documento_parte_envolvida AS string) AS documento_parte_envolvida,
    safe_cast (matricula_responsavel AS int64) AS matricula_responsavel,
    safe_cast (_prefect_extracted_at AS date) AS datalake_transformed_at
from {{ source("brutos_sicop_staging","parte_envolvida") }}
