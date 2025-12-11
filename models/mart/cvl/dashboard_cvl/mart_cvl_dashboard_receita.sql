{{
    config(
        alias='receita'
    )
}}

SELECT 
    r.*, 
    CONCAT(r.referencia_mes, ' ', r.referencia_ano) AS referencia_mes_ano,
    T.receita_item,
    f.nome_fantasia AS NOME_OS,
    j.tipo,
    s.secretaria AS SECRETARIA,
    c.numero_contrato,
    CONCAT(h.codigo_cc, '', h.digito_cc) AS contabancaria
FROM {{ ref('raw_adm_contrato_gestao__receita_dados') }} AS r

LEFT JOIN {{ ref('raw_adm_contrato_gestao__administracao_unidade') }} AS f 
    ON r.cod_unidade = f.cod_unidade
LEFT JOIN {{ ref('raw_adm_contrato_gestao__receita_item') }} AS T 
    ON T.id_receita_item = r.id_item
LEFT JOIN {{ ref('raw_adm_contrato_gestao__conta_bancaria') }} AS h 
    ON r.id_conta_bancaria = h.id_conta_bancaria
LEFT JOIN {{ ref('raw_adm_contrato_gestao__conta_bancaria_tipo') }} AS j 
    ON j.id_conta_bancaria_tipo = h.cod_tipo
LEFT JOIN {{ ref('raw_adm_contrato_gestao__contrato') }} AS c 
    ON r.id_instrumento_contratual = c.id_contrato
LEFT JOIN {{ ref('raw_adm_contrato_gestao__secretaria') }} AS s 
    ON c.id_secretaria = s.id_secretaria
