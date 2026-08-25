{{
    config(
        alias="funcionarios_sms",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        }
    )
}}

with 

secretarias as (
  select 
    s.id_setor as secretaria_id,
    ifnull(s.nome_completo, s.nome) as secretaria_nome,
    s.sigla as secretaria_sigla,
    s.data_inicio as secretaria_inicio,
    s.data_fim as secretaria_fim
  from {{ ref("raw_recursos_humanos_ergon__setor") }} s
  where s.id_setor = s.id_secretaria
),

setor_sms as (
  select 
    s.id_setor,
    s.data_inicio as setor_inicio,
    s.data_fim as setor_fim,
    s.id_setor_pai,
    ifnull(s.nome_completo, nome) as setor_nome,
    s.sigla as setor_sigla,
    struct(
      s.id_secretaria as secretaria_id,
      se.secretaria_sigla,
      se.secretaria_nome
    ) as secretaria,
    struct(
      s.id_empresa as empresa_id,
      e.nome_empresa as empresa_nome,
      e.cnpj as empresa_cnpj,
      e.sigla as empresa_sigla
    ) as empresa
  from {{ ref("raw_recursos_humanos_ergon__setor") }} s
  inner join {{ ref("raw_recursos_humanos_ergon__empresas") }} e
    on e.id_empresa = s.id_empresa
  left join secretarias se
    on se.secretaria_id = s.id_secretaria
    and s.data_inicio between se.secretaria_inicio and ifnull(se.secretaria_fim, current_date("America/Sao_Paulo"))
  where s.id_secretaria in ('1800', '1851')
  or s.id_empresa in ('32', '80', '81', '82', '83', '84', '85', '86', '87', '88', '89', '97', '23')
), 

vinculos as (
  select 
    v.id_funcionario, 
    v.id_vinculo, 
    v.id_matricula_vinculo,
    v.data_vacancia,
    v.motivo_vacancia,
    tv.nome as tipo_vinculo,
    v.regime_juridico,
    v.data_exercicio
  from {{ ref("raw_recursos_humanos_ergon__vinculo") }} v
  inner join {{ ref("raw_recursos_humanos_ergon__tipo_vinculo") }} tv
    on tv.sigla = v.tipo_vinculo
),

ultimo_provimento_sms_de_cada_vinculo as (
  select
    p.id_funcionario,
    struct(
      p.id_vinculo as vinculo_id,
      v.id_matricula_vinculo as vinculo_matricula,
      p.data_inicio as provimento_inicio,
      p.data_fim as provimento_fim,
      v.tipo_vinculo as vinculo_tipo,
      v.regime_juridico as regime_juridico_vinculo,
      v.data_exercicio as data_exercicio_vinculo,
      case 
        when v.data_vacancia is null and p.data_fim is null
        then true
        else false
      end as status_ativo,
      struct(
        v.data_vacancia as vacancia_data,
        v.motivo_vacancia as vacancia_motivo
      ) as vacancia_vinculo,
      struct(
        s.id_setor as setor_id,
        s.setor_sigla,
        s.setor_nome,
        s.empresa,
        s.secretaria
      ) as lotacao,
      struct(
        c.id_cargo as cargo_id,
        c.nome as cargo_nome,
        c.categoria as cargo_categoria,
        c.subcategoria as cargo_subcategoria
      ) as cargo
    ) as vinculos
  from {{ ref("raw_recursos_humanos_ergon__provimento") }}  p
  inner join setor_sms s 
    on s.empresa.empresa_id = p.id_empresa 
    and s.id_setor = p.id_setor
    and p.data_inicio between s.setor_inicio and ifnull(s.setor_fim, '9999-12-31') --current_date("America/Sao_Paulo"))
  inner join {{ ref("raw_recursos_humanos_ergon__cargo") }} c 
    on c.id_cargo = p.id_cargo
  inner join vinculos v
    on v.id_funcionario = p.id_funcionario
    and v.id_vinculo = p.id_vinculo
  qualify
    row_number() over (
        partition by p.id_funcionario, p.id_vinculo order by p.data_inicio desc
    ) = 1
)


select 
  f.cpf, 
  safe_cast(f.cpf as int64) as cpf_particao,
  f.nome_funcionario,
  f.id_funcionario,
  count(distinct p.vinculos.vinculo_id) as contagem_vinculos,
  array_agg(p.vinculos order by p.vinculos.vinculo_id) as vinculos
from ultimo_provimento_sms_de_cada_vinculo p 
inner join {{ ref("raw_recursos_humanos_ergon__funcionario") }} f 
  on f.id_funcionario = p.id_funcionario
group by f.cpf, f.nome_funcionario, f.id_funcionario
