{{
  config(
    materialized='table',
    alias='certidao_habite_se'
  )
}}

SELECT
    Num_Hab AS id_habite_se,
    Num_Proc AS numero_processo,
    Num_Lic AS id_licenciamento,
    Habite_se AS numero_certidao,
    Cod_DLF AS id_orgao_sislic,
    CAST(Dt_Emissao AS DATETIME) AS data_emissao,
    CAST(Dt_Certidao AS DATETIME) AS data_certidao,
    Matricula_RGI AS matricula_rgi,
    Nr_Oficio_RGI AS numero_oficio_rgi,
    Pal AS numero_pal,
    DescricaoLote AS descricao_lote,
    Numeracao_Habitavel AS endereco_habitavel,
    Mat_Tec_Resp AS matricula_tecnico_responsavel,
    CAST(Cancelado AS BOOL) AS cancelado,
    CAST(Habite_se_Total AS BOOL) AS habite_se_total,
    OBS AS observacao,

FROM {{ source('adm_licenca_urbanismo_staging', 'certidao_habite_se') }}
