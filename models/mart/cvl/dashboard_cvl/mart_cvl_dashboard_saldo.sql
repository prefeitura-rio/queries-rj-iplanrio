{{
    config(
        alias='saldo'
    )
}}

SELECT 
    r.*,
    CONCAT(r.referencia_mes_receita, ' ', r.referencia_ano_receita) AS referencia_mes_ano,
    j.tipo,
    s.secretaria AS SECRETARIA,
    c.numero_contrato,
    f.saldo_item,
    CONCAT(h.codigo_cc, '', COALESCE(h.digito_cc, '')) AS contabancaria,
    t.nome_fantasia AS NOME_OS,
    p.banco
FROM {{ ref('raw_adm_contrato_gestao__saldo_dados') }} AS r
LEFT JOIN {{ ref('raw_adm_contrato_gestao__saldo_item') }} AS f 
    ON r.id_saldo_item = f.id_saldo_item
LEFT JOIN {{ ref('raw_adm_contrato_gestao__conta_bancaria') }} AS h 
    ON r.id_conta_bancaria = h.id_conta_bancaria
LEFT JOIN {{ ref('raw_adm_contrato_gestao__conta_bancaria_tipo') }} AS j 
    ON j.id_conta_bancaria_tipo = h.cod_tipo
LEFT JOIN {{ ref('raw_adm_contrato_gestao__contrato') }} AS c 
    ON r.id_instrumento_contratual = c.id_contrato
LEFT JOIN {{ ref('raw_adm_contrato_gestao__secretaria') }} AS s 
    ON c.id_secretaria = s.id_secretaria
LEFT JOIN {{ ref('raw_adm_contrato_gestao__administracao_unidade') }} AS t 
    ON h.cod_instituicao = t.cod_unidade
LEFT JOIN {{ ref('raw_adm_contrato_gestao__conta_bancaria') }} AS m 
    ON r.id_conta_bancaria = m.id_conta_bancaria
LEFT JOIN {{ ref('raw_adm_contrato_gestao__agencia') }} AS n 
    ON m.id_agencia = n.id_agencia
LEFT JOIN {{ ref('raw_adm_contrato_gestao__banco') }} AS p 
    ON n.id_banco = p.id_banco
