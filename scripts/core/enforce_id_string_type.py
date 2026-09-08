#!/usr/bin/env python3
"""
Script para garantir que todas as colunas que começam com 'id_' sejam do tipo STRING.

Este script:
1. Varre todos os arquivos SQL em busca de colunas que começam com 'id_'
2. Verifica se estão usando clean_and_cast com tipo 'string'
3. Corrige casts incorretos (int64, integer, etc.) para string
4. Atualiza os YAMLs correspondentes para data_type: string

Uso:
    python enforce_id_string_type.py [--dry-run] [--path CAMINHO] [--verbose]

Exemplos:
    # Modo dry-run (apenas mostra o que seria alterado)
    python enforce_id_string_type.py --dry-run

    # Aplicar em uma pasta específica
    python enforce_id_string_type.py --path models/raw/sma

    # Aplicar em todos os modelos
    python enforce_id_string_type.py
"""

import re
import argparse
from pathlib import Path
from typing import List, Tuple, Dict, Set
import sys
import yaml

# Importar analisador de contexto
try:
    from core.sql_context_analyzer import (
        should_apply_transformation,
        is_column_from_source,
        analyze_sql_file,
        get_model_level
    )
    HAS_CONTEXT_ANALYZER = True
except ImportError:
    try:
        from sql_context_analyzer import (
            should_apply_transformation,
            is_column_from_source,
            analyze_sql_file,
            get_model_level
        )
        HAS_CONTEXT_ANALYZER = True
    except ImportError:
        HAS_CONTEXT_ANALYZER = False
        print("⚠️  Aviso: sql_context_analyzer não encontrado. Transformações serão aplicadas em todos os níveis.")
        print("   Para análise inteligente, certifique-se que sql_context_analyzer.py está no mesmo diretório.")


# Padrões para detectar definições de colunas id_*
PATTERNS = {
    # {{ clean_and_cast('coluna', 'TIPO_ERRADO') }} AS id_coluna
    'macro_wrong_type': re.compile(
        r"{{\s*clean_and_cast\s*\(\s*['\"](\w+)['\"]\s*,\s*['\"](?!string)(\w+)['\"]\s*(?:,\s*[^)]+)?\s*\)\s*}}\s+AS\s+(id_\w+)",
        re.IGNORECASE
    ),

    # SAFE_CAST(REGEXP_REPLACE(...) AS TIPO_ERRADO) AS id_coluna
    'safe_cast_regexp': re.compile(
        r"SAFE_CAST\s*\(\s*REGEXP_REPLACE\s*\([^)]+\)\s+AS\s+(?!STRING)(\w+)\s*\)\s+AS\s+(id_\w+)",
        re.IGNORECASE | re.DOTALL
    ),

    # SAFE_CAST(coluna AS TIPO_ERRADO) AS id_coluna
    'safe_cast_simple': re.compile(
        r"SAFE_CAST\s*\(\s*(\w+)\s+AS\s+(?!STRING)(\w+)\s*\)\s+AS\s+(id_\w+)",
        re.IGNORECASE
    ),

    # CAST(coluna AS TIPO_ERRADO) AS id_coluna (sem SAFE)
    'cast_simple': re.compile(
        r"(?<!SAFE_)CAST\s*\(\s*(\w+)\s+AS\s+(?!STRING)(\w+)\s*\)\s+AS\s+(id_\w+)",
        re.IGNORECASE
    ),

    # coluna AS id_coluna (sem cast)
    'no_cast': re.compile(
        r"^\s*(\w+)\s+AS\s+(id_\w+)\s*[,\n]",
        re.MULTILINE
    ),
}


def should_transform_column(
    content: str,
    id_col: str,
    line_num: int,
    original: str,
    check_context: bool
) -> Tuple[bool, str]:
    """
    Verifica se uma coluna id_* deve ser transformada baseado no contexto.

    Returns:
        Tupla (should_transform, reason)
    """
    if not check_context or not HAS_CONTEXT_ANALYZER:
        return True, "Análise de contexto desabilitada"

    # Verificar se vem de source ou ref
    lines = content.split('\n')
    sql_line = lines[line_num - 1] if line_num <= len(lines) else ""

    try:
        should_apply, reason = should_apply_transformation(
            content, id_col, line_num, sql_line
        )
        return should_apply, reason
    except Exception as e:
        # Em caso de erro, aplicar transformação (comportamento conservador)
        return True, f"Erro na análise de contexto: {e}"


def find_id_columns_in_sql(file_path: Path, check_context: bool = True) -> Tuple[Dict[str, Dict], Dict[str, Dict]]:
    """
    Encontra todas as colunas id_* em um arquivo SQL e verifica se estão corretas.

    Args:
        file_path: Caminho do arquivo SQL
        check_context: Se True, verifica contexto (source vs ref) antes de marcar como issue

    Returns:
        Tupla (issues, skipped) onde:
        - issues: Dict com informações sobre as colunas que precisam de correção
        - skipped: Dict com informações sobre as colunas ignoradas por contexto
    """
    try:
        content = file_path.read_text(encoding='utf-8')
    except (OSError, IOError, UnicodeDecodeError) as e:
        return {}, {}

    issues = {}
    skipped = {}  # Para debug: colunas que foram ignoradas por contexto

    # Padrão 1: macro com tipo errado
    for match in PATTERNS['macro_wrong_type'].finditer(content):
        source_col = match.group(1)
        wrong_type = match.group(2)
        id_col = match.group(3)
        line_num = content[:match.start()].count('\n') + 1

        # Verificar contexto antes de adicionar como issue
        should_fix, reason = should_transform_column(
            content, id_col, line_num, match.group(0), check_context
        )

        if should_fix:
            issues[id_col] = {
                'line': line_num,
                'type': 'macro_wrong_type',
                'source_column': source_col,
                'current_type': wrong_type,
                'expected_type': 'string',
                'original': match.group(0),
                'fix': f"{{{{ clean_and_cast('{source_col}', 'string') }}}} AS {id_col}",
                'reason': reason
            }
        else:
            skipped[id_col] = {'line': line_num, 'reason': reason}

    # Padrão 2: SAFE_CAST com REGEXP_REPLACE
    for match in PATTERNS['safe_cast_regexp'].finditer(content):
        wrong_type = match.group(1)
        id_col = match.group(2)
        line_num = content[:match.start()].count('\n') + 1

        # Extrair o nome da coluna de dentro do REGEXP_REPLACE
        inner_match = re.search(r'REGEXP_REPLACE\s*\(\s*(?:TRIM\s*\(\s*)?(?:CAST\s*\(\s*)?(\w+)', match.group(0), re.IGNORECASE)
        source_col = inner_match.group(1) if inner_match else 'unknown'

        # Detectar se tem TRIM
        has_trim = 'TRIM' in match.group(0).upper()

        should_fix, reason = should_transform_column(
            content, id_col, line_num, match.group(0), check_context
        )

        if should_fix:
            issues[id_col] = {
                'line': line_num,
                'type': 'safe_cast_regexp',
                'source_column': source_col,
                'current_type': wrong_type,
                'expected_type': 'string',
                'original': match.group(0).strip(),
                'fix': f"{{{{ clean_and_cast('{source_col}', 'string'{', trim=true' if has_trim else ''}) }}}} AS {id_col}",
                'reason': reason
            }
        else:
            skipped[id_col] = {'line': line_num, 'reason': reason}

    # Padrão 3: SAFE_CAST simples
    for match in PATTERNS['safe_cast_simple'].finditer(content):
        source_col = match.group(1)
        wrong_type = match.group(2)
        id_col = match.group(3)
        line_num = content[:match.start()].count('\n') + 1

        should_fix, reason = should_transform_column(
            content, id_col, line_num, match.group(0), check_context
        )

        if should_fix:
            issues[id_col] = {
                'line': line_num,
                'type': 'safe_cast_simple',
                'source_column': source_col,
                'current_type': wrong_type,
                'expected_type': 'string',
                'original': match.group(0),
                'fix': f"{{{{ clean_and_cast('{source_col}', 'string') }}}} AS {id_col}",
                'reason': reason
            }
        else:
            skipped[id_col] = {'line': line_num, 'reason': reason}

    # Padrão 4: CAST sem SAFE
    for match in PATTERNS['cast_simple'].finditer(content):
        source_col = match.group(1)
        wrong_type = match.group(2)
        id_col = match.group(3)
        line_num = content[:match.start()].count('\n') + 1

        should_fix, reason = should_transform_column(
            content, id_col, line_num, match.group(0), check_context
        )

        if should_fix:
            issues[id_col] = {
                'line': line_num,
                'type': 'cast_simple',
                'source_column': source_col,
                'current_type': wrong_type,
                'expected_type': 'string',
                'original': match.group(0),
                'fix': f"{{{{ clean_and_cast('{source_col}', 'string', safe=false) }}}} AS {id_col}",
                'reason': reason
            }
        else:
            skipped[id_col] = {'line': line_num, 'reason': reason}

    # Padrão 5: sem cast (precisa adicionar)
    for match in PATTERNS['no_cast'].finditer(content):
        source_col = match.group(1)
        id_col = match.group(2)
        line_num = content[:match.start()].count('\n') + 1

        # Ignorar se já for um macro ou cast
        if '{{' not in match.group(0) and 'CAST' not in match.group(0).upper():
            should_fix, reason = should_transform_column(
                content, id_col, line_num, match.group(0), check_context
            )

            if should_fix:
                issues[id_col] = {
                    'line': line_num,
                    'type': 'no_cast',
                    'source_column': source_col,
                    'current_type': 'unknown',
                    'expected_type': 'string',
                    'original': match.group(0).strip(),
                    'fix': f"{{{{ clean_and_cast('{source_col}', 'string') }}}} AS {id_col},",
                    'reason': reason
                }
            else:
                skipped[id_col] = {'line': line_num, 'reason': reason}

    return issues, skipped


def fix_sql_file(file_path: Path, issues: Dict, dry_run: bool = False) -> int:
    """
    Corrige os problemas encontrados em um arquivo SQL.

    Returns:
        Número de correções aplicadas
    """
    if not issues or dry_run:
        return len(issues)

    try:
        content = file_path.read_text(encoding='utf-8')

        # Ordenar por linha em ordem reversa para não afetar posições
        sorted_issues = sorted(issues.items(), key=lambda x: x[1]['line'], reverse=True)

        for col_name, issue in sorted_issues:
            # Substituir usando regex para garantir que pegamos a ocorrência certa
            # Usar o original como padrão de busca
            original_escaped = re.escape(issue['original'])
            content = re.sub(original_escaped, issue['fix'], content, count=1)

        file_path.write_text(content, encoding='utf-8')
        return len(issues)

    except Exception as e:
        print(f"❌ Erro ao corrigir {file_path}: {e}")
        return 0


def find_yaml_for_sql(sql_path: Path) -> Path:
    """Encontra o arquivo YAML correspondente a um SQL."""
    # Geralmente o YAML tem o mesmo nome ou está no mesmo diretório
    yaml_path = sql_path.with_suffix('.yml')
    if yaml_path.exists():
        return yaml_path

    # Tentar com .yaml
    yaml_path = sql_path.with_suffix('.yaml')
    if yaml_path.exists():
        return yaml_path

    # Buscar no mesmo diretório por YAMLs que referenciem o modelo
    model_name = sql_path.stem
    for yml_file in sql_path.parent.glob('*.yml'):
        try:
            with open(yml_file, 'r', encoding='utf-8') as f:
                content = f.read()
                if f"name: {model_name}" in content:
                    return yml_file
        except (OSError, IOError, UnicodeDecodeError):
            continue

    return None


def fix_yaml_column_types(yaml_path: Path, columns: Set[str], dry_run: bool = False) -> int:
    """
    Corrige os tipos de colunas id_* nos arquivos YAML.

    Returns:
        Número de correções aplicadas
    """
    if not yaml_path or not yaml_path.exists():
        return 0

    try:
        with open(yaml_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        changes = 0
        new_lines = []
        i = 0

        while i < len(lines):
            line = lines[i]

            # Procurar por definição de coluna id_*
            col_match = re.match(r'(\s*)-\s+name:\s+(id_\w+)', line)

            if col_match and col_match.group(2) in columns:
                indent = col_match.group(1)
                new_lines.append(line)
                i += 1

                # Procurar data_type nas próximas linhas
                found_data_type = False
                while i < len(lines):
                    next_line = lines[i]

                    # Se chegou em outra coluna ou seção, parar
                    if re.match(r'\s*-\s+name:', next_line) or not next_line.strip():
                        break

                    # Verificar se é data_type
                    dt_match = re.match(r'(\s*)data_type:\s*(\w+)', next_line)
                    if dt_match:
                        current_type = dt_match.group(2)
                        if current_type.lower() != 'string':
                            # Substituir por string
                            new_lines.append(f"{dt_match.group(1)}data_type: string\n")
                            changes += 1
                            found_data_type = True
                        else:
                            new_lines.append(next_line)
                            found_data_type = True
                        i += 1
                        break
                    else:
                        new_lines.append(next_line)
                        i += 1

                # Se não encontrou data_type, adicionar
                if not found_data_type:
                    new_lines.append(f"{indent}    data_type: string\n")
                    changes += 1
            else:
                new_lines.append(line)
                i += 1

        if changes > 0 and not dry_run:
            with open(yaml_path, 'w', encoding='utf-8') as f:
                f.writelines(new_lines)

        return changes

    except Exception as e:
        print(f"❌ Erro ao processar YAML {yaml_path}: {e}")
        return 0


def find_sql_files(base_path: Path, pattern: str = '*.sql') -> List[Path]:
    """Encontra todos os arquivos SQL recursivamente."""
    return list(base_path.rglob(pattern))


def main():
    parser = argparse.ArgumentParser(
        description='Garante que todas as colunas id_* sejam do tipo STRING',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  %(prog)s --dry-run
  %(prog)s --path models/raw/sma
  %(prog)s --verbose
        """
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Mostra o que seria alterado sem modificar os arquivos'
    )

    parser.add_argument(
        '--path',
        type=str,
        default='models',
        help='Caminho base para buscar arquivos SQL (padrão: models)'
    )

    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Mostra todas as correções feitas'
    )

    parser.add_argument(
        '--skip-yaml',
        action='store_true',
        help='Não atualiza arquivos YAML'
    )

    parser.add_argument(
        '--no-context-check',
        action='store_true',
        help='Desabilita análise inteligente de contexto (aplica em todos os níveis)'
    )

    args = parser.parse_args()

    # Determinar caminho base
    script_dir = Path(__file__).parent.parent
    base_path = script_dir / args.path

    if not base_path.exists():
        print(f"❌ Caminho não encontrado: {base_path}")
        sys.exit(1)

    print(f"🔍 Buscando arquivos .sql em: {base_path}")
    sql_files = find_sql_files(base_path)
    print(f"📁 Encontrados {len(sql_files)} arquivos SQL")

    if args.dry_run:
        print("\n⚠️  MODO DRY-RUN - Nenhum arquivo será modificado\n")

    total_sql_issues = 0
    total_yaml_changes = 0
    files_with_issues = 0
    yamls_updated = 0

    for sql_file in sql_files:
        issues, skipped = find_id_columns_in_sql(sql_file, check_context=not args.no_context_check)

        if issues:
            files_with_issues += 1
            total_sql_issues += len(issues)

            rel_path = sql_file.relative_to(script_dir)
            print(f"\n{'🔍' if args.dry_run else '✅'} {rel_path}")
            print(f"  {len(issues)} coluna(s) id_* com tipo incorreto")

            if args.verbose or args.dry_run:
                for col_name, issue in issues.items():
                    print(f"\n  📍 Linha {issue['line']}: {col_name}")
                    print(f"    Tipo atual: {issue['current_type']}")
                    print(f"    Antes: {issue['original']}")
                    print(f"    Depois: {issue['fix']}")

            # Corrigir SQL
            fixed = fix_sql_file(sql_file, issues, dry_run=args.dry_run)

            # Corrigir YAML correspondente
            if not args.skip_yaml:
                yaml_path = find_yaml_for_sql(sql_file)
                if yaml_path:
                    yaml_changes = fix_yaml_column_types(
                        yaml_path,
                        set(issues.keys()),
                        dry_run=args.dry_run
                    )
                    if yaml_changes > 0:
                        yamls_updated += 1
                        total_yaml_changes += yaml_changes
                        rel_yaml = yaml_path.relative_to(script_dir)
                        print(f"  📝 YAML: {rel_yaml} ({yaml_changes} correção(ões))")

    # Resumo
    print("\n" + "="*80)
    print(f"📊 RESUMO:")
    print(f"  Arquivos SQL processados: {len(sql_files)}")
    print(f"  Arquivos SQL com problemas: {files_with_issues}")
    print(f"  Total de colunas id_* corrigidas: {total_sql_issues}")

    if not args.skip_yaml:
        print(f"  Arquivos YAML atualizados: {yamls_updated}")
        print(f"  Total de data_type corrigidos: {total_yaml_changes}")

    if args.dry_run and (total_sql_issues > 0 or total_yaml_changes > 0):
        print(f"\n💡 Execute sem --dry-run para aplicar as mudanças")
    elif total_sql_issues > 0 or total_yaml_changes > 0:
        print(f"\n✅ Correções aplicadas com sucesso!")
        print(f"\n⚠️  IMPORTANTE: Execute 'dbt compile' para validar as mudanças")
    else:
        print(f"\n✨ Nenhuma correção necessária - todas as colunas id_* estão corretas!")


if __name__ == '__main__':
    main()
