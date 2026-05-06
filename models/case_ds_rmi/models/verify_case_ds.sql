-- 1. Verificação de Contagem de Linhas (Garantir que as tabelas não estão vazias)
SELECT 'aluno' as tabela, COUNT(*) as total FROM `rj-crm-registry-dev.case_ds.aluno`
UNION ALL
SELECT 'avaliacao' as tabela, COUNT(*) as total FROM `rj-crm-registry-dev.case_ds.avaliacao`
UNION ALL
SELECT 'escola' as tabela, COUNT(*) as total FROM `rj-crm-registry-dev.case_ds.escola`
UNION ALL
SELECT 'frequencia' as tabela, COUNT(*) as total FROM `rj-crm-registry-dev.case_ds.frequencia`
UNION ALL
SELECT 'turma' as tabela, COUNT(*) as total FROM `rj-crm-registry-dev.case_ds.turma`;

-- 2. Verificação de Nulos em Colunas Críticas
SELECT 
    'aluno' as tabela,
    COUNTIF(id_aluno IS NULL) as nulos_id_aluno,
    COUNTIF(id_turma IS NULL) as nulos_id_turma,
FROM `rj-crm-registry-dev.case_ds.aluno`
UNION ALL
SELECT 
    'avaliacao' as tabela,
    COUNTIF(id_aluno IS NULL) as nulos_id_aluno,
    COUNTIF(id_turma IS NULL) as nulos_id_turma,
    NULL as nulos_id_escola
FROM `rj-crm-registry-dev.case_ds.avaliacao`;

-- 3. Verificação de Integridade Referencial (Joins)
-- Todos os alunos nas tabelas de fato existem na tabela de dimensão aluno?
SELECT 
    'alunos_em_avaliacao_sem_cadastro' as teste,
    COUNT(DISTINCT av.id_aluno) as count
FROM `rj-crm-registry-dev.case_ds.avaliacao` av
LEFT JOIN `rj-crm-registry-dev.case_ds.aluno` al ON av.id_aluno = al.id_aluno
WHERE al.id_aluno IS NULL;

SELECT 
    'alunos_em_frequencia_sem_cadastro' as teste,
    COUNT(DISTINCT f.id_aluno) as count
FROM `rj-crm-registry-dev.case_ds.frequencia` f
LEFT JOIN `rj-crm-registry-dev.case_ds.aluno` al ON f.id_aluno = al.id_aluno
WHERE al.id_aluno IS NULL;

-- 4. Verificação de Anonimização (Exemplo: Verificar se os IDs parecem Hashes)
SELECT 
    id_aluno, 
    id_turma, 
    id_escola,
    LENGTH(id_aluno) as len_id -- Deve ser o comprimento de um hash de 64 bits/FARM_FINGERPRINT
FROM `rj-crm-registry-dev.case_ds.aluno`
LIMIT 5;

-- 5. Verificação de Ranges de Dados (Datas anonimizadas para o ano 2000)
SELECT 
    MIN(data_inicio) as min_data, 
    MAX(data_fim) as max_data 
FROM `rj-crm-registry-dev.case_ds.frequencia`;

-- 6. Verificação de Notas e Frequência (Valores válidos)
SELECT 
    MIN(disciplina_1) as min_nota, 
    MAX(disciplina_1) as max_nota,
    MIN(frequencia) as min_freq,
    MAX(frequencia) as max_freq
FROM `rj-crm-registry-dev.case_ds.avaliacao` av
JOIN `rj-crm-registry-dev.case_ds.frequencia` f ON av.id_aluno = f.id_aluno;


-- 7. Contagem de Linhas com Joins (Garantir que as chaves batem entre as tabelas)
SELECT 
    'aluno + avaliacao' as juncao,
    COUNT(*) as total_linhas
FROM `rj-crm-registry-dev.case_ds.aluno` al
INNER JOIN `rj-crm-registry-dev.case_ds.avaliacao` av ON al.id_aluno = av.id_aluno

UNION ALL

SELECT 
    'aluno + frequencia' as juncao,
    COUNT(*) as total_linhas
FROM `rj-crm-registry-dev.case_ds.aluno` al
INNER JOIN `rj-crm-registry-dev.case_ds.frequencia` f ON al.id_aluno = f.id_aluno

UNION ALL

SELECT 
    'aluno + escola' as juncao,
    COUNT(*) as total_linhas
FROM `rj-crm-registry-dev.case_ds.aluno` al
INNER JOIN `rj-crm-registry-dev.case_ds.escola` e ON al.id_escola = e.id_escola

UNION ALL

SELECT 
    'aluno + avaliacao + frequencia + escola' as juncao,
    COUNT(*) as total_linhas
FROM `rj-crm-registry-dev.case_ds.aluno` al
INNER JOIN `rj-crm-registry-dev.case_ds.avaliacao` av ON al.id_aluno = av.id_aluno
INNER JOIN `rj-crm-registry-dev.case_ds.frequencia` f ON al.id_aluno = f.id_aluno AND av.id_aluno = f.id_aluno
INNER JOIN `rj-crm-registry-dev.case_ds.escola` e ON al.id_escola = e.id_escola;

-- 8. Verificação de Cobertura (LEFT JOIN para identificar órfãos)
SELECT 
    COUNT(al.id_aluno) as total_alunos,
    COUNT(av.id_aluno) as alunos_com_avaliacao,
    COUNT(f.id_aluno) as alunos_com_frequencia,
    SAFE_DIVIDE(COUNT(av.id_aluno), COUNT(al.id_aluno)) * 100 as perc_cobertura_avaliacao
FROM `rj-crm-registry-dev.case_ds.aluno` al
LEFT JOIN `rj-crm-registry-dev.case_ds.avaliacao` av ON al.id_aluno = av.id_aluno
LEFT JOIN `rj-crm-registry-dev.case_ds.frequencia` f ON al.id_aluno = f.id_aluno;
