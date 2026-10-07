{{
    config(
        schema="projeto_bi_dtgg",
        alias='dimensao_setor',
        materialized="table",
        tags=["mart", "projeto_bi_dtgg", "recursos_humanos", "dtgg", "setores"],
        description="Tabela que contém a dimensão de setores, com relacionamento hierárquico, de modo que é possível estabelecer relação de subordinação e o caminho até chegar num setor específico."
    )
}}

with recursive setores_iplan as (
  select concat(repeat("0", 15 - length(raiz.id_setor)), raiz.id_setor) as ordem,
    to_hex(md5(concat(repeat("0", 15 - length(raiz.id_setor)), raiz.id_setor))) as id_setor_hash,
    raiz.id_setor as caminho,
    raiz.id_setor,
    raiz.id_empresa,
    raiz.id_secretaria,
    raiz.id_setor_pai,
    raiz.sigla, 
    raiz.nome_completo,
    raiz.data_inicio,
    raiz.data_fim, 
    1 as nivel, 
    extinto
  from {{ ref("raw_ergon_iplanrio__setor") }} raiz
  where id_setor in ('1153') --, '1451', '5651') - comentado, pois apenas o código 1153 será considerado para a IplanRio
  and ifnull(data_fim, current_date("America/Sao_Paulo")) between '2025-01-01' and '9999-12-31' -- Definindo histórico de registros a partir de 2025.

  union all

  select concat(ordem, repeat("0", 15 - length(ramos.id_setor)), ramos.id_setor), 
    to_hex(md5(concat(ordem, repeat("0", 15 - length(ramos.id_setor)), ramos.id_setor))),
    concat(setores_iplan.caminho, "->", ramos.id_setor),
    ramos.id_setor,
    ramos.id_empresa,
    ramos.id_secretaria,
    ramos.id_setor_pai,
    ramos.sigla, 
    ramos.nome_completo,
    ramos.data_inicio,
    ramos.data_fim,
    setores_iplan.nivel + 1,
    ramos.extinto 
  from {{ ref("raw_ergon_iplanrio__setor") }} ramos
  inner join setores_iplan 
    on ramos.id_setor_pai = setores_iplan.id_setor
    and ramos.id_empresa = setores_iplan.id_empresa
    and ramos.data_inicio between setores_iplan.data_inicio and ifnull(setores_iplan.data_fim, current_date("America/Sao_Paulo"))
)

select distinct ordem, 
  id_setor_hash,
  caminho,
  id_setor, 
  id_setor_pai, 
  id_empresa, 
  id_secretaria, 
  sigla,
  nome_completo,
  nivel
from setores_iplan 
order by ordem
