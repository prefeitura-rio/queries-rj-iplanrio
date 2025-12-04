{{
    config(
        alias='despesas'
    )
}}

SELECT 
    d.*, 
    s.secretaria AS SECRETARIA, 
    f.nome_fantasia AS NOME_OS,
    CONCAT(d.referencia_mes, ' ', d.referencia_ano) AS referencia_mes_ano,
    CONCAT(pc.cod_item_plano_de_contas, ' ', pc.descricao_item_plano_de_contas) AS despesas,
pc.cod_item_plano_de_contas,
    c.numero_contrato,
    j.tipo,
    CONCAT(h.codigo_cc, '', COALESCE(h.digito_cc, '')) AS contabancaria
FROM {{ ref('raw_adm_contrato_gestao__despesas') }} AS d
LEFT JOIN {{ ref('raw_adm_contrato_gestao__contrato') }} AS c 
    ON d.id_contrato = c.id_contrato
LEFT JOIN {{ ref('raw_adm_contrato_gestao__secretaria') }} AS s 
    ON c.id_secretaria = s.id_secretaria
LEFT JOIN {{ ref('raw_adm_contrato_gestao__plano_contas') }} AS pc 
    ON d.id_despesa = pc.id_item_plano_de_contas
LEFT JOIN {{ ref('raw_adm_contrato_gestao__administracao_unidade') }} AS f 
    ON d.cod_organizacao = f.cod_unidade
LEFT JOIN {{ ref('raw_adm_contrato_gestao__conta_bancaria') }} AS h 
    ON d.id_conta_bancaria = h.id_conta_bancaria
LEFT JOIN {{ ref('raw_adm_contrato_gestao__conta_bancaria_tipo') }} AS j 
    ON j.id_conta_bancaria_tipo = h.cod_tipo
