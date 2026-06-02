{{
  config(
    materialized='table',
    alias='endereco_produto_pedido'
  )
}}

SELECT
codendereco AS id_endereco,
codlogra AS id_logradouro,
numporta AS numero_porta,
complporta AS complemento_porta,
codbairro AS id_bairro,
numpal AS numero_pal,
numquadra AS numero_quadra,
numlote AS numero_lote,
CAST(vfmunicipal AS BOOL) AS endereco_dentro_municipio,
uf AS uf,
numpaa AS numero_paa,
CAST(dtcadastro AS DATETIME) AS data_cadastro,
CAST(CAST(matriccadastrador AS FLOAT64) AS int64) AS matricula_cadastrador,
cep AS cep,
inscricaoimovel AS inscricao_imovel,
CAST(CAST(id_tpinscricaoimovel AS FLOAT64) AS int64) AS id_tipo_inscricao_imovel,

FROM {{ source('adm_licenca_urbanismo_staging', 'endereco_produto_pedido') }}
