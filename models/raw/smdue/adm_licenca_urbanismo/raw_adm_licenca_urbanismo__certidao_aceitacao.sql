{{
  config(
    materialized='table',
    alias='certidao_aceitacao'
  )
}}

SELECT
    Num_Aceitacao AS id_aceitacao,
    Num_Proc AS numero_processo,
    Num_Lic AS id_licenciamento,
    Aceitacao AS numero_certidao,
    Cod_DLF AS id_orgao_sislic,
    CAST(Dt_Emissao AS DATETIME) AS data_emissao,
    CAST(Dt_Certidao AS DATETIME)  AS data_certidao,
    Matricula_RGI AS matricula_rgi,
    Nr_Oficio_RGI AS numero_oficio_rgi,
    PAL AS numero_pal,
    DescricaoLote AS descricao_lote,
    Mat_Tec_Resp AS matricula_tecnico_responsavel,
    CAST(Cancelado AS BOOL) AS cancelado,
    Obs AS observacao

FROM {{ source('adm_licenca_urbanismo_staging', 'certidao_aceitacao') }}
