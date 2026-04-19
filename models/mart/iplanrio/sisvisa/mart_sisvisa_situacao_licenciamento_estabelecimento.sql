{{
    config(
        schema="sisvisa",
        alias='situacao_licenciamento_estabelecimento',
        materialized="table",
        tags=["mart", "sisvisa", "estabelecimento", "situacao_licenciamento"],
        description="Tabela que contém a situação de licenciamento de todos os estabelecimentos que estão cadastrados no SISVISA, e que tiveram algum licenciamento ativo."
    )
}}

with 

--DEFINIÇÃO DE TIPOS DE SITUAÇÃO DO LICENCIAMENTO DOS ESTABELECIMENTOS
situacao_licenciamento AS (
  SELECT *
  FROM UNNEST([
    STRUCT(
      1 AS id_situacao,
      CONCAT(
        'LICENCIAMENTO VIGENTE - COMPETÊNCIA ',
        CAST(EXTRACT(YEAR FROM CURRENT_DATE()) AS STRING),
        '/',
        CAST(EXTRACT(YEAR FROM CURRENT_DATE()) + 1 AS STRING)
      ) AS nome_situacao
    ),
    STRUCT(
      2 AS id_situacao,
      CONCAT(
        'LICENCIAMENTO ATÉ 30/04/',
        CAST(EXTRACT(YEAR FROM CURRENT_DATE()) AS STRING)
      ) AS nome_situacao
    ),
    STRUCT(
      3 AS id_situacao,
      'LICENCIAMENTO PENDENTE ANOS ANTERIORES' AS nome_situacao
    )
  ])
),

--APURAÇÃO DOS ULTIMOS REQUERIMENTOS E LICENCIAMENTOS DE CADA ESTABELECIMENTO
apuracao_licenciamento as
(
	select e.Id as id_estabelecimento_sisvisa, 
    e.InscricaoMunicipal as inscricao_municipal,
    e.cpfcnpj as cnpj, 
    e.Email,
		r.data_ultimo_requerimento_valido, 
		h1.data_validade_ult_licenca_ativa, 
		h2.data_validade_ult_licenca_expirada,
		case 
			when extract(year from data_validade_ult_licenca_ativa) > extract(year from current_date())
			then 1
			when extract(year from data_validade_ult_licenca_ativa) = extract(year from current_date())
			then 2
			else 3
		end as id_situacao
	from {{ source('brutos_sisvisa_staging', 'Estabelecimento') }} e
  inner join  (
			SELECT a.EstabelecimentoId, MAX(a.DataInicio) AS data_ultimo_requerimento_valido 
			from {{ source('brutos_sisvisa_staging', 'RequerimentoAutodeclaracao') }} a
			INNER join {{ source('brutos_sisvisa_staging', 'CobrancaSisvisa') }} b on b.ID_REQUERIMENTO = a.Id and b.Situacao = 0
			where a.SituacaoId in (7, 10, 11, 14)
			group by a.EstabelecimentoId) r on r.EstabelecimentoId = e.Id
	left join (
				select EstabelecimentoId, max(DataDeValidade) as data_validade_ult_licenca_ativa
				from {{ source('brutos_sisvisa_staging', 'HistoricoDeLicenca') }}
				where SituacaoDaLicenca = 1
				group by EstabelecimentoId
				) h1 on h1.EstabelecimentoId = r.EstabelecimentoId
	left join (
				select EstabelecimentoId, max(DataDeValidade) as data_validade_ult_licenca_expirada
				from {{ source('brutos_sisvisa_staging', 'HistoricoDeLicenca') }}
				where SituacaoDaLicenca = 6
				group by EstabelecimentoId
				) h2 on h2.EstabelecimentoId = r.EstabelecimentoId
),

--APURAÇÃO DE VALORES COBRADOS NO ÚLTIMO REQUERIMENTO
--FOI NECESSÁRIO SOMAR, POIS HÁ CASOS DE MAIS DE UMA COBRANÇA VÁLIDA PARA UM MESMO REQUERIMENTO
valores_cobrados as (
	select a.id_estabelecimento_sisvisa, q.DataInicio, q.total_cobranca_ultimo_licenciamento
	from apuracao_licenciamento a
	inner join
			(
				SELECT a.EstabelecimentoId, DataInicio, sum(c.VL_PRINCIPAL) as total_cobranca_ultimo_licenciamento
				from {{ source('brutos_sisvisa_staging', 'RequerimentoAutodeclaracao') }} a
				INNER join {{ source('brutos_sisvisa_staging', 'CobrancaSisvisa') }} c on c.ID_REQUERIMENTO = a.Id and c.Situacao = 0
				where a.SituacaoId in (7, 10, 11, 14)
				group by a.EstabelecimentoId, DataInicio
			) q on q.EstabelecimentoId = a.id_estabelecimento_sisvisa and q.DataInicio = a.data_ultimo_requerimento_valido
),

--CONJUNTO DA ÚLTIMA SITUAÇÃO CADASTRAL DE CADA EMPRESA NO BCADASTRO
ultima_situacao_do_estabelecimento_bcadastro as (
  select a.cnpj, max(a.situacao_cadastral.data) as data_ultima_situacao
  from {{ ref('raw_bcadastro_cnpj') }} a
  group by a.cnpj
),

--CONJUNTO DE EMPRESAS ATUALMENTE ATIVAS NO BCADASTRO, COM ENDEREÇO NO MUNICÍPIO DO RIO DE JANEIRO
estabelecimentos_ativos_bcadastro as (
  select a.cnpj, a.razao_social, a.nome_fantasia, a.contato, a.responsavel, a.endereco,
    a.situacao_cadastral.id as situacao_id, a.situacao_cadastral.data as data_situacao, a.situacao_cadastral.descricao as descricao
  from {{ ref('raw_bcadastro_cnpj') }} a
  inner join ultima_situacao_do_estabelecimento_bcadastro b on b.cnpj = a.cnpj 
        and b.data_ultima_situacao = a.situacao_cadastral.data
        and a.situacao_cadastral.id = '2'
  where a.endereco.id_municipio = '6001' --apenas do Rio de Janeiro
),

--CONJUNTO DE ESTABELECIMENTOS ISENTOS NO SISVISA, COM BASE NA VIEW DE RESTRIÇÕES DE ALVARÁS DO SINAE (OCORRÊNCIA DO CÓDIGO 05 NAS RESTRIÇÕES)
estabelecimentos_isentos_ivisa as (
  select distinct a.INSCRICAOMUNICIPAL as inscricao_municipal
  from {{ source('brutos_sisvisa_staging', 'Vw_Rest_Alvara_Estab_Sinae_Datalake') }} a
  where a.CODIGO = '05'
)

select safe_cast(a.id_estabelecimento_sisvisa as string) as id_estabelecimento_sisvisa,  
  b.cnpj, 
  safe_cast(a.inscricao_municipal as string) as inscricao_municipal, 
  b.nome_fantasia, 
  b.razao_social, 
  ifnull(b.contato.email, a.Email) as email_estabelecimento, 
  array_length(b.contato.telefone) as quantidade_telefones_estabelecimento, 
  b.contato.telefone as telefones_estabelecimento,
  struct(
    b.endereco.tipo_logradouro,
    b.endereco.logradouro,
    b.endereco.numero,
    b.endereco.complemento,
    b.endereco.bairro,
    b.endereco.cep,
    b.endereco.municipio_nome,
    b.endereco.uf) as endereco_estabelecimento,
  b.responsavel.cpf as cpf_responsavel, 
  d.nome as nome_responsavel, 
  d.contato.email as email_responsavel,
  d.contato.telefone as telefone_responsavel,
  struct(
    d.endereco.tipo_logradouro,
    d.endereco.logradouro,
    d.endereco.numero,
    d.endereco.complemento,
    d.endereco.bairro,
    d.endereco.cep,
    d.endereco.municipio,
    d.endereco.uf) as endereco_responsavel,
  a.data_ultimo_requerimento_valido, 
  a.data_validade_ult_licenca_ativa, 
  a.data_validade_ult_licenca_expirada,
  a.id_situacao,
  s.nome_situacao,
  v.total_cobranca_ultimo_licenciamento
from apuracao_licenciamento a
inner join situacao_licenciamento s on s.id_situacao = a.id_situacao
inner join estabelecimentos_ativos_bcadastro b on b.cnpj = a.cnpj
inner join valores_cobrados v on v.id_estabelecimento_sisvisa = a.id_estabelecimento_sisvisa
left join estabelecimentos_isentos_ivisa c on safe_cast(c.inscricao_municipal as int64) = a.inscricao_municipal
left join {{ ref('raw_bcadastro_cpf') }} d on d.cpf = b.responsavel.cpf
where c.inscricao_municipal is null
