{{
    config(
        alias="ViewDatalake_ouvidoria",
        materialized="table",
        tags=["raw", "chamado", "ViewDatalake_ouvidoria", "1746"],
        description="Tabela que representa a ViewDatalake_ouvidoria que existe no banco de dados de origem do 1746. Seu código foi transcrito para cá para resolver o problema de ingestão, uma vez que a execução do comando da view é extremamente lento no banco de dados de origem."
    )
}}

with 

reclamacoes as (
	SELECT id_chamado_pai_fk,
       COUNT(1) as quantidade_reclamacoes
    FROM {{ source('brutos_1746_staging', 'tb_chamado_vinculado') }} c
	WHERE ic_vinculo = 'O'
   group by id_chamado_pai_fk
),

classificacao_atual as (
  SELECT id_chamado_fk,
    MAX(id_classificacao_chamado) as id_classificacao_atual
  FROM {{ source('brutos_1746_staging', 'tb_classificacao_chamado') }}
  group by id_chamado_fk
),

ultimo_andamento as (
  SELECT id_chamado_fk, 
		MAX(id_andamento) as id_ult_andamento
  FROM {{ source('brutos_1746_staging', 'tb_andamento') }}
  group by id_chamado_fk
)

SELECT
	c.id_chamado					as id_chamado,						-- id_chamado: Identificador único do chamado no banco de dados.
	c.id_origem_ocorrencia_fk		as id_origem_ocorrencia,				-- id_origem_ocorrencia: Identificador da origem da ocorrência.
	c.dt_inicio						as data_inicio,						-- data_inicio: Data de abertura do chamado (quando o operador registra o chamado).
	c.dt_fim						as data_fim,							-- data_fim: Data de fechamento do chamado (quando atendido ou quando não pode ser atendido).
	b.id_bairro						as id_bairro,							-- id_bairro: Identificador único do bairro onde ocorreu o fato.
	tra.id_territorialidade_fk		as id_territorialidade,				-- id_territorialidade: Identificador único da territorialidade (região da cidade com órgão responsável, ex.: CDURP).
	bl.id_logradouro_fk				as id_logradouro,						-- id_logradouro: Identificador único do logradouro onde ocorreu o fato.
	c.ds_endereco_numero			as numero_logradouro,					-- numero_logradouro: Número do logradouro onde ocorreu o fato.
	rc.id_unidade_organizacional_fk	as id_unidade_organizacional,			-- id_unidade_organizacional: Identificador único do órgão executor do chamado (ex.: COMLURB).
	uo.no_unidade_organizacional	as nome_unidade_organizacional,		-- nome_unidade_organizacional: Nome do órgão executor da demanda.
	uo.id_unidade_pai_fk			as id_unidade_organizacional_mae,		-- id_unidade_organizacional_mae: Identificador da unidade organizacional mãe.
	uo.fl_ouvidoria					as unidade_organizacional_ouvidoria,	-- unidade_organizacional_ouvidoria: Indica se o chamado foi feito pela Ouvidoria (1 = sim, 0 = não).
	ca.no_categoria					as categoria,							-- categoria: Categoria do chamado (ex.: serviço, informação, sugestão, elogio, reclamação, crítica).
	t.id_tipo						as id_tipo,							-- id_tipo: Identificador do tipo do chamado.
	t.no_tipo						as tipo,								-- tipo: nome do tipo do chamado (ex.: Iluminação pública).
	s.id_subtipo					as id_subtipo,						-- id_subtipo: Identificador do subtipo.
	s.no_subtipo					as subtipo,							-- subtipo: nome do subtipo (ex.: Reparo de lâmpada apagada).
	st.no_status					as status,							-- status: Status do chamado (ex.: fechado com solução, em andamento, pendente).
	c.nu_coord_x					as longitude,							-- longitude: Localização do evento.
	c.nu_coord_y					as latitude,							-- latitude: Localização do evento.
	cs.dt_alvo_finalizacao			as data_alvo_finalizacao,				-- data_alvo_finalizacao: Data prevista para finalização.
	cs.dt_alvo_diagnostico			as data_alvo_diagnostico,				-- data_alvo_diagnostico: Data prevista para diagnóstico (quando aplicável).
	cc.dt_real_diagnostico			as data_real_diagnostico,				-- data_real_diagnostico: Data em que o diagnóstico foi realizado (quando aplicável).
	c.nu_prazo						as tempo_prazo,						-- tempo_prazo: Prazo para execução do serviço (em dias ou horas).
	c.ic_prazo_unidade_tempo		as prazo_unidade,						-- prazo_unidade: Unidade de tempo utilizada no prazo (D ou H).
	c.ic_prazo_tipo					as prazo_tipo,						-- prazo_tipo: Tipo de prazo (D = diagnóstico ou F = finalização).
	d.ds_ic							as dentro_prazo,						-- dentro_prazo: Indica se o chamado está dentro do prazo estipulado.
	case 
        when c.dt_fim is null then 'Não Encerrado' 
        else 'Encerrado'
	END as situacao,							-- situacao: Indica se o chamado foi encerrado.
	st.no_status_view				as tipo_situacao,						-- tipo_situacao: Status atual (Atendido, Atendido parcialmente, Não atendido, Não constatado, Andamento).
	j.no_justificativa				as justificativa_status,				-- justificativa_status: Justificativa usada pelo órgão ao definir o status.
	ifnull(r.quantidade_reclamacoes, 0) as reclamacoes,						-- reclamacoes: Quantidade de reclamações.
	c.ds_chamado					as descricao,							-- descricao: Descrição do chamado.
	c.dt_atualizacao				as dt_atualizacao						-- atualização: Data da última atualização do chamado.
from {{ source('brutos_1746_staging', 'tb_chamado') }} c
left join reclamacoes r 
    on r.id_chamado_pai_fk = c.id_chamado
left join {{ source('brutos_1746_staging', 'tb_bairro_logradouro') }} bl   
    on c.id_bairro_logradouro_fk = bl.id_bairro_logradouro
left join {{ source('brutos_1746_staging', 'tb_bairro') }} b    
    on b.id_bairro = bl.id_bairro_fk
left join {{ source('brutos_1746_staging', 'tb_territorialidade_regiao_administrativa_bairro') }} trab 
    on b.id_bairro = trab.id_bairro_fk
left join {{ source('brutos_1746_staging', 'tb_territorialidade_regiao_administrativa') }} tra  
    on trab.id_territorialidade_regiao_administrativa_fk = tra.id_territorialidade_regiao_administrativa
inner join {{ source('brutos_1746_staging', 'tb_responsavel_chamado') }} rc   
    on c.id_responsavel_chamado_fk = rc.id_responsavel_chamado
inner join {{ source('brutos_1746_staging', 'tb_unidade_organizacional') }} uo   
    on rc.id_unidade_organizacional_fk = uo.id_unidade_organizacional
inner join classificacao_atual cla
    on cla.id_chamado_fk = c.id_chamado
inner join {{ source('brutos_1746_staging', 'tb_classificacao_chamado') }} cc   
    on cc.id_chamado_fk = cla.id_chamado_fk
    and cc.id_classificacao_chamado = cla.id_classificacao_atual
inner join {{ source('brutos_1746_staging', 'tb_classificacao') }} cl   
    on cc.id_classificacao_fk = cl.id_classificacao
inner join {{ source('brutos_1746_staging', 'tb_categoria') }} ca   
    on cl.id_categoria_fk = ca.id_categoria
inner join {{ source('brutos_1746_staging', 'tb_subtipo') }} s    
    on cl.id_subtipo_fk = s.id_subtipo
inner join {{ source('brutos_1746_staging', 'tb_tipo') }} t
    on s.id_tipo_fk = t.id_tipo
inner join {{ source('brutos_1746_staging', 'tb_status') }} st
    on c.id_status_fk = st.id_status
inner join {{ source('brutos_1746_staging', 'tb_chamado_sla') }} cs
    on c.id_chamado = cs.id_chamado_fk
inner join {{ source('brutos_1746_staging', 'tb_dicionario_ic') }} d
    on trim(cs.ic_status_sla_finalizacao) = trim(d.ic) AND d.id_dicionario_ic IN (83, 84, 85, 86, 87, 88)
left join ultimo_andamento ua
    on ua.id_chamado_fk = c.id_chamado
left join {{ source('brutos_1746_staging', 'tb_andamento') }} a    
    on a.id_chamado_fk = ua.id_chamado_fk 
    AND a.id_andamento = ua.id_ult_andamento
left join {{ source('brutos_1746_staging', 'tb_justificativa') }} j
    on a.id_justificativa_fk = j.id_justificativa
WHERE uo.id_instituicao_fk = 3
and uo.fl_ouvidoria = TRUE