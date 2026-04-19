{{
    config(
        schema="sisvisa",
        alias='estabelecimento_nunca_licenciado',
        materialized="table",
        tags=["mart", "sisvisa", "estabelecimento", "nunca_licenciado"],
        description="Tabela que contém os estabelecimentos que estão cadastrados no SISVISA, mas que nunca foram tiveram licenciamento ativo."
    )
}}

with 

--REGISTROS DE REQUERIMENTOS VÁLIDOS NO SISVISA. REQUERIMENTOS VÁLIDOS SÃO TODOS QUE FORAM PAGOS E ESTÃO OU ESTIVERAM ATIVOS E VIGENTES EM ALGUM MOMENTO
requerimentos_validos as (
  select a.id as requerimento_id, a.EstabelecimentoId, b.DT_COMPETENCIA as data_competencia, b.DT_PAGAMENTO as data_pagamento, 
         ifnull(b.VL_PRINCIPAL, 0) as valor_principal, ifnull(b.VL_RECEITA, 0) as valor_receita, ifnull(b.VL_MORA, 0) as valor_mora, 
         ifnull(b.VL_MULTA, 0) as valor_multa, ifnull(b.VL_PRINCIPALDESC, 0) as valor_desconto
  from {{ source('brutos_sisvisa_staging', 'RequerimentoAutodeclaracao') }} a
  inner join {{ source('brutos_sisvisa_staging', 'CobrancaSisvisa') }} b on b.ID_REQUERIMENTO = a.Id and b.SITUACAO = 0
  where a.SituacaoId in (7, 10, 11, 14)
),

--REGISTROS DE LICENÇAS QUE ESTÃO VIGENTES OU QUE ESTIVERAM VIGENTES E AGORA ESTÃO EXPIRADAS
historico_licencas_validas as (
  select a.EstabelecimentoId, a.Licenciamento, a.DataDeCriacao, a.DataDeValidade
  from {{ source('brutos_sisvisa_staging', 'HistoricoDeLicenca') }} a
  where a.SituacaoDaLicenca in (1, 6)
),

--REGISTROS DE ESTABELECIMENTOS NO CADASTRO DO SISVISA E QUE NUNCA TIVERAM UM REQUERIMENTO DE LICENCIAMENTO VÁLIDO
estabelecimentos_sisvisa_nunca_licenciados as (
  select distinct a.EstabelecimentoId, b.CpfCnpj as cnpj, b.ativo, b.InscricaoMunicipal as inscricao_municipal, b.RazaoSocial, b.NomeFantasia, b.Email
  from requerimentos_validos a
  inner join {{ source('brutos_sisvisa_staging', 'Estabelecimento') }} b on b.id = a.EstabelecimentoId
  left join historico_licencas_validas c on c.EstabelecimentoId = b.id
  where c.EstabelecimentoId is null
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

--PRIMEIRA VERSÃO DA CONSULTA DO CONJUNTO DE EMPRESAS, SEM O TRATAMENTO DE CNPJ DUPLICADO NO SISVISA
--NÃO É POSSÍVEL TIRAR A DUPLICIDADE IDENTIFICADA, POIS PRECISO DA INCRIÇÃO MUNICIPAL PARA CRUZAR COM A ISENÇÃO PELA RESTRIÇÃO DE ALVARÁ DO SINAE
select safe_cast(a.EstabelecimentoId as string) as id_estabelecimento_sisvisa,
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
        b.endereco.municipio_nome as municipio,
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
from estabelecimentos_sisvisa_nunca_licenciados a
inner join estabelecimentos_ativos_bcadastro b on b.cnpj = a.cnpj
left join estabelecimentos_isentos_ivisa c on safe_cast(c.inscricao_municipal as int64) = a.inscricao_municipal
left join {{ ref('raw_bcadastro_cpf') }} d on d.cpf = b.responsavel.cpf
where c.inscricao_municipal is null



/*
AINDA FALTA O CONJUNTO DE ATIVIDADES ECONÔMICAS REGULADAS PELO IVISA PARA IDENTIFICAR MAIS ESTABELECIMENTOS OMISSOS NO SISVISA QUE NÃO ESTÃO ISENTOS DO PAGAMENTO DE LICENÇA
*/
