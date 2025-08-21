{{
    config(
        alias='usuario_sistema',
        schema='compras_materiais_servicos_sigma'
    )
}}

SELECT
    SAFE_CAST(cd_orgao_designacao AS STRING) AS codigo_orgao_designacao,
    SAFE_CAST(cd_perfil AS STRING) AS codigo_perfil,
    SAFE_CAST(cd_terminal AS STRING) AS codigo_terminal,
    SAFE_CAST(cpf AS STRING) AS cpf_colaborador,
    SAFE_CAST(ds_orgao_designacao AS STRING) AS descricao_orgao_designacao,
    SAFE_CAST(ds_perfil AS STRING) AS descricao_perfil,
    SAFE_CAST(dt_inclusao AS STRING) AS data_inclusao,
    SAFE_CAST(dt_ultima_sessao AS STRING) AS data_ultima_sessao,
    SAFE_CAST(email_alternativo AS STRING) AS email_alternativo,
    SAFE_CAST(email_institucional AS STRING) AS email_institucional,
    SAFE_CAST(hora_acesso_fim AS STRING) AS hora_acesso_fim,
    SAFE_CAST(hora_acesso_ini AS STRING) AS hora_acesso_inicio,
    SAFE_CAST(hora_ultima_sessao AS STRING) AS hora_ultima_sessao,
    SAFE_CAST(matricula AS STRING) AS matricula,
    SAFE_CAST(minuto_acesso_fim AS STRING) AS minuto_acesso_fim,
    SAFE_CAST(minuto_acesso_ini AS STRING) AS minuto_acesso_inicio,
    SAFE_CAST(nm_funcionario AS STRING) AS nome_servidor,
    SAFE_CAST(nm_prestador_servico AS STRING) AS empresa_contratante,
    SAFE_CAST(privilegio_almoxarifado AS STRING) AS privilegio_almoxarifado,
    SAFE_CAST(st_situacao AS STRING) AS status_situacao,
    SAFE_CAST(st_status AS STRING) AS status,
    SAFE_CAST(tel_alternativo1 AS STRING) AS telefone_alternativo_1,
    SAFE_CAST(tel_alternativo2 AS STRING) AS telefone_alternativo_2,
    SAFE_CAST(tel_corporativo1 AS STRING) AS telefone_corporativo_1,
    SAFE_CAST(tel_corporativo2 AS STRING) AS telefone_corporativo_2,
from {{ source('compras_materiais_servicos_sigma_staging', 'VW_USUARIO_SISTEMA')}}