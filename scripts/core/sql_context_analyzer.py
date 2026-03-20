#!/usr/bin/env python3
"""
Módulo para analisar o contexto de queries SQL em modelos dbt.

Identifica se uma coluna vem de:
- {{ source(...) }} - transformação DEVE ser aplicada
- {{ ref(...) }} - transformação NÃO deve ser aplicada (já foi aplicada no modelo origem)
- Tabela hardcoded - transformação DEVE ser aplicada
"""

import re
from typing import List, Dict, Tuple, Optional
from pathlib import Path


def extract_ctes_and_main_query(sql_content: str) -> Dict[str, str]:
    """
    Extrai CTEs (WITH clauses) e a query principal de um SQL.

    Returns:
        Dict mapeando nome_cte -> conteúdo_sql
    """
    ctes = {}

    # Remover comentários
    sql_clean = re.sub(r'--[^\n]*', '', sql_content)
    sql_clean = re.sub(r'/\*.*?\*/', '', sql_clean, flags=re.DOTALL)

    # Encontrar WITH clause
    with_pattern = re.compile(
        r'\bWITH\s+(\w+)\s+AS\s*\((.*?)\)(?=\s*,\s*\w+\s+AS\s*\(|\s*SELECT)',
        re.IGNORECASE | re.DOTALL
    )

    for match in with_pattern.finditer(sql_clean):
        cte_name = match.group(1)
        cte_content = match.group(2)
        ctes[cte_name.lower()] = cte_content

    return ctes


def find_from_source(sql_snippet: str) -> List[str]:
    """
    Identifica se o SQL seleciona de um {{ source(...) }}.

    Returns:
        Lista de nomes de sources encontrados
    """
    source_pattern = re.compile(
        r"{{\s*source\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*}}",
        re.IGNORECASE
    )

    sources = []
    for match in source_pattern.finditer(sql_snippet):
        schema = match.group(1)
        table = match.group(2)
        sources.append(f"{schema}.{table}")

    return sources


def find_from_ref(sql_snippet: str) -> List[str]:
    """
    Identifica se o SQL seleciona de um {{ ref(...) }}.

    Returns:
        Lista de nomes de refs encontrados
    """
    ref_pattern = re.compile(
        r"{{\s*ref\s*\(\s*['\"]([^'\"]+)['\"]\s*\)\s*}}",
        re.IGNORECASE
    )

    refs = []
    for match in ref_pattern.finditer(sql_snippet):
        model_name = match.group(1)
        refs.append(model_name)

    return refs


def find_from_hardcoded(sql_snippet: str) -> List[str]:
    """
    Identifica se o SQL seleciona de uma tabela hardcoded (sem source/ref).

    Returns:
        Lista de nomes de tabelas hardcoded
    """
    # Procurar por FROM seguido de algo que não é source/ref
    from_pattern = re.compile(
        r'\bFROM\s+([`\w\.]+)',
        re.IGNORECASE
    )

    tables = []
    for match in from_pattern.finditer(sql_snippet):
        table_ref = match.group(1)
        # Ignorar se for um CTE ou subquery
        if '{{' not in sql_snippet[max(0, match.start()-50):match.end()+50]:
            # Verificar se não é um alias ou CTE
            if '.' in table_ref or '`' in table_ref:
                tables.append(table_ref.strip('`'))

    return tables


def find_sql_block_boundaries(sql_content: str, line_num: int) -> Tuple[int, int]:
    """
    Encontra os limites do bloco SQL (CTE ou query) que contém a linha.

    Args:
        sql_content: Conteúdo completo do SQL
        line_num: Número da linha (1-indexed)

    Returns:
        Tupla (start_line, end_line) do bloco
    """
    lines = sql_content.split('\n')

    # Ir para trás até encontrar SELECT ou WITH ... AS (
    start_line = line_num - 1
    paren_depth = 0

    for i in range(line_num - 1, -1, -1):
        line = lines[i]

        # Contar parênteses (de trás para frente, então invertemos)
        paren_depth += line.count(')')
        paren_depth -= line.count('(')

        # Se encontramos SELECT no nível certo, achamos o início
        if paren_depth <= 0 and re.search(r'\bSELECT\b', line, re.IGNORECASE):
            start_line = i
            break

        # Ou se encontramos o início de um CTE: "nome AS ("
        if re.search(r'\w+\s+AS\s*\(', line, re.IGNORECASE):
            start_line = i
            break

    # Ir para frente até encontrar o fim do bloco
    end_line = len(lines) - 1
    paren_depth = 0

    for i in range(start_line, len(lines)):
        line = lines[i]

        # Contar parênteses
        paren_depth += line.count('(')
        paren_depth -= line.count(')')

        # Se encontramos fechamento de parênteses que fecha um CTE
        if paren_depth < 0:
            end_line = i
            break

        # Se encontramos próximo CTE (indica fim do bloco atual)
        if i > start_line and re.search(r',\s*\w+\s+AS\s*\(', line, re.IGNORECASE):
            end_line = i - 1
            break

        # Se encontramos ponto-e-vírgula ou fim de statement
        if ';' in line and paren_depth == 0:
            end_line = i
            break

    return start_line, min(end_line + 1, len(lines))


def is_column_from_source(sql_content: str, column_name: str, line_num: int) -> bool:
    """
    Verifica se uma coluna específica está sendo selecionada de um source.

    Args:
        sql_content: Conteúdo completo do arquivo SQL
        column_name: Nome da coluna a verificar (ex: 'id_funcionario')
        line_num: Número da linha onde a coluna aparece

    Returns:
        True se a coluna vem de um source (precisa transformação)
        False se a coluna vem de um ref (já foi transformada)
    """
    lines = sql_content.split('\n')

    # Encontrar os limites do bloco SQL que contém esta coluna
    start_line, end_line = find_sql_block_boundaries(sql_content, line_num)

    # Extrair apenas o bloco relevante
    block_lines = lines[start_line:end_line]
    block_content = '\n'.join(block_lines)

    # Procurar FROM dentro desse bloco
    from_match = None
    for line in block_lines:
        if re.search(r'\bFROM\s+', line, re.IGNORECASE):
            from_match = line
            break

    if not from_match:
        # Se não encontrou FROM, assumir que precisa transformação
        return True

    # Verificar se o FROM é de source, ref ou hardcoded
    has_source = bool(find_from_source(from_match))
    has_ref = bool(find_from_ref(from_match))

    # Se tem source ou não tem ref, precisa transformação
    # Se tem ref, NÃO precisa transformação (já foi aplicada)
    return has_source or not has_ref


def is_column_being_transformed(sql_line: str, column_name: str) -> bool:
    """
    Verifica se a coluna está sendo transformada (CAST, função, etc.)
    ou apenas sendo passada adiante.

    Args:
        sql_line: Linha do SQL onde a coluna aparece
        column_name: Nome da coluna

    Returns:
        True se está sendo transformada (precisa correção)
        False se está apenas sendo selecionada (não precisa correção)
    """
    # Se a linha tem CAST, SAFE_CAST, clean_and_cast, etc., está sendo transformada
    if re.search(r'\b(CAST|SAFE_CAST|clean_and_cast|REGEXP_REPLACE)\s*\(', sql_line, re.IGNORECASE):
        return True

    # Se é apenas "coluna AS outro_nome" ou "coluna," não está sendo transformada
    simple_select = re.match(
        rf'^\s*[\w\.]+\s+AS\s+{column_name}\s*[,\n]',
        sql_line.strip(),
        re.IGNORECASE
    )

    return not bool(simple_select)


def should_apply_transformation(
    sql_content: str,
    column_name: str,
    line_num: int,
    sql_line: str
) -> Tuple[bool, str]:
    """
    Determina se uma transformação deve ser aplicada a uma coluna.

    Args:
        sql_content: Conteúdo completo do SQL
        column_name: Nome da coluna (ex: 'id_funcionario')
        line_num: Número da linha onde aparece
        sql_line: Linha específica do SQL

    Returns:
        Tupla (should_apply, reason)
        - should_apply: True se deve aplicar transformação
        - reason: Razão para aplicar ou não
    """
    # Regra 1: Se está sendo transformada (tem CAST, etc.), verificar origem
    is_transformed = is_column_being_transformed(sql_line, column_name)

    if is_transformed:
        # Está sendo transformada, verificar se vem de source
        from_source = is_column_from_source(sql_content, column_name, line_num)

        if from_source:
            return True, "Coluna sendo transformada de source/tabela hardcoded"
        else:
            return False, "Coluna sendo re-transformada de ref (já foi transformada antes)"

    else:
        # Não está sendo transformada, apenas passada adiante
        return False, "Coluna apenas sendo selecionada sem transformação"


def analyze_sql_file(file_path: Path) -> Dict[str, any]:
    """
    Analisa um arquivo SQL completo para entender seu contexto.

    Returns:
        Dict com informações sobre sources, refs e tabelas hardcoded
    """
    try:
        content = file_path.read_text(encoding='utf-8')
    except (OSError, IOError, UnicodeDecodeError):
        return {}

    return {
        'sources': find_from_source(content),
        'refs': find_from_ref(content),
        'hardcoded': find_from_hardcoded(content),
        'has_transformations': bool(re.search(r'\b(CAST|SAFE_CAST|clean_and_cast)\s*\(', content, re.IGNORECASE)),
        'ctes': extract_ctes_and_main_query(content)
    }


def get_model_level(file_path: Path) -> str:
    """
    Determina o nível do modelo (raw, staging, mart, etc.)
    baseado no caminho do arquivo.

    Returns:
        'raw', 'staging', 'mart', ou 'unknown'
    """
    path_str = str(file_path).lower()

    if '/raw/' in path_str:
        return 'raw'
    elif '/staging/' in path_str:
        return 'staging'
    elif '/mart/' in path_str or '/intermediate/' in path_str:
        return 'mart'
    else:
        return 'unknown'


if __name__ == '__main__':
    # Testes
    test_sql_source = """
    SELECT
        SAFE_CAST(NUMFUNC AS int64) AS id_funcionario,
        nome
    FROM {{ source('ergon', 'funcionarios') }}
    """

    test_sql_ref = """
    SELECT
        id_funcionario,
        nome
    FROM {{ ref('raw_funcionario') }}
    """

    test_sql_ref_transform = """
    SELECT
        CAST(id_funcionario AS STRING) AS id_funcionario,
        nome
    FROM {{ ref('raw_funcionario') }}
    """

    print("Test 1 - Source:")
    print("Sources:", find_from_source(test_sql_source))
    print("Refs:", find_from_ref(test_sql_source))

    print("\nTest 2 - Ref:")
    print("Sources:", find_from_source(test_sql_ref))
    print("Refs:", find_from_ref(test_sql_ref))

    print("\nTest 3 - Ref with transform:")
    print("Sources:", find_from_source(test_sql_ref_transform))
    print("Refs:", find_from_ref(test_sql_ref_transform))
