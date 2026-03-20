#!/usr/bin/env python3
"""
Script para substituir padrões de SAFE_CAST(REGEXP_REPLACE(...))
pelo macro clean_and_cast em arquivos SQL do dbt.

Uso:
    python replace_with_clean_and_cast_macro.py [--dry-run] [--path CAMINHO]

Exemplos:
    # Modo dry-run (apenas mostra o que seria alterado)
    python replace_with_clean_and_cast_macro.py --dry-run

    # Aplicar mudanças em uma pasta específica
    python replace_with_clean_and_cast_macro.py --path models/raw/sma/recursos_humanos_ergon

    # Aplicar mudanças em todos os modelos
    python replace_with_clean_and_cast_macro.py
"""

import re
import argparse
from pathlib import Path
from typing import List, Tuple, Dict
import sys

# Importar módulo de análise de contexto SQL
try:
    from core.sql_context_analyzer import should_apply_transformation
except ImportError:
    try:
        from sql_context_analyzer import should_apply_transformation
    except ImportError:
        # Se não conseguir importar, definir função dummy
        def should_apply_transformation(sql_content, column_name, line_num, sql_line):
            return True, "Context analyzer not available"


# Padrões regex para identificar as transformações
# \s+ permite quebras de linha e múltiplos espaços
PATTERNS = [
    # Padrão 1: SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(coluna AS STRING)), r'\.0$', '') AS tipo)
    re.compile(
        r"SAFE_CAST\s*\(\s*REGEXP_REPLACE\s*\(\s*TRIM\s*\(\s*CAST\s*\(\s*(\w+)\s+AS\s+STRING\s*\)\s*\),\s*r'\\\.0\$'\s*,\s*''\s*\)\s+AS\s+(\w+)\s*\)",
        re.IGNORECASE | re.DOTALL
    ),
    # Padrão 2: SAFE_CAST(REGEXP_REPLACE(TRIM(coluna), r'\.0$', '') AS tipo)
    re.compile(
        r"SAFE_CAST\s*\(\s*REGEXP_REPLACE\s*\(\s*TRIM\s*\(\s*(\w+)\s*\)\s*,\s*r'\\\.0\$'\s*,\s*''\s*\)\s+AS\s+(\w+)\s*\)",
        re.IGNORECASE | re.DOTALL
    ),
    # Padrão 3: SAFE_CAST(REGEXP_REPLACE(CAST(coluna AS STRING), r'\.0$', '') AS tipo)
    re.compile(
        r"SAFE_CAST\s*\(\s*REGEXP_REPLACE\s*\(\s*CAST\s*\(\s*(\w+)\s+AS\s+STRING\s*\)\s*,\s*r'\\\.0\$'\s*,\s*''\s*\)\s+AS\s+(\w+)\s*\)",
        re.IGNORECASE | re.DOTALL
    ),
    # Padrão 4: SAFE_CAST(REGEXP_REPLACE(coluna, r'\.0$', '') AS tipo) - Multi-linha
    re.compile(
        r"SAFE_CAST\s*\(\s*REGEXP_REPLACE\s*\(\s*(\w+)\s*,\s*r'\\\.0\$'\s*,\s*''\s*\)\s+AS\s+(\w+)\s*\)",
        re.IGNORECASE | re.DOTALL
    ),
    # Padrão 5: CAST sem SAFE - CAST(REGEXP_REPLACE(TRIM(CAST(coluna AS STRING)), r'\.0$', '') AS tipo)
    re.compile(
        r"(?<!SAFE_)CAST\s*\(\s*REGEXP_REPLACE\s*\(\s*TRIM\s*\(\s*CAST\s*\(\s*(\w+)\s+AS\s+STRING\s*\)\s*\)\s*,\s*r'\\\.0\$'\s*,\s*''\s*\)\s+AS\s+(\w+)\s*\)",
        re.IGNORECASE | re.DOTALL
    ),
    # Padrão 6: CAST sem SAFE - CAST(REGEXP_REPLACE(CAST(coluna AS STRING), r'\.0$', '') AS tipo)
    re.compile(
        r"(?<!SAFE_)CAST\s*\(\s*REGEXP_REPLACE\s*\(\s*CAST\s*\(\s*(\w+)\s+AS\s+STRING\s*\)\s*,\s*r'\\\.0\$'\s*,\s*''\s*\)\s+AS\s+(\w+)\s*\)",
        re.IGNORECASE | re.DOTALL
    ),
]


def detect_pattern(text: str) -> Tuple[int, re.Match]:
    """
    Detecta qual padrão corresponde ao texto.

    Returns:
        Tupla (índice_do_padrão, match_object) ou (None, None) se não encontrar
    """
    for idx, pattern in enumerate(PATTERNS):
        match = pattern.search(text)
        if match:
            return idx, match
    return None, None


def generate_macro_call(pattern_idx: int, column_name: str, data_type: str) -> str:
    """
    Gera a chamada do macro baseado no padrão detectado.

    Args:
        pattern_idx: Índice do padrão que foi detectado
        column_name: Nome da coluna
        data_type: Tipo de dados de destino

    Returns:
        String com a chamada do macro
    """
    # Padrão 0: TRIM + CAST
    if pattern_idx == 0:
        return f"{{{{ clean_and_cast('{column_name}', '{data_type.lower()}', trim=true) }}}}"

    # Padrão 1: TRIM apenas
    elif pattern_idx == 1:
        return f"{{{{ clean_and_cast('{column_name}', '{data_type.lower()}', trim=true) }}}}"

    # Padrão 2: CAST apenas
    elif pattern_idx == 2:
        return f"{{{{ clean_and_cast('{column_name}', '{data_type.lower()}') }}}}"

    # Padrão 3: Sem TRIM nem CAST explícito
    elif pattern_idx == 3:
        return f"{{{{ clean_and_cast('{column_name}', '{data_type.lower()}') }}}}"

    # Padrão 4: CAST sem SAFE + TRIM
    elif pattern_idx == 4:
        return f"{{{{ clean_and_cast('{column_name}', '{data_type.lower()}', trim=true, safe=false) }}}}"

    # Padrão 5: CAST sem SAFE
    elif pattern_idx == 5:
        return f"{{{{ clean_and_cast('{column_name}', '{data_type.lower()}', safe=false) }}}}"

    return None


def process_file(file_path: Path, dry_run: bool = False, check_context: bool = True) -> Tuple[int, List[str], int]:
    """
    Processa um arquivo SQL substituindo os padrões pelo macro.

    Args:
        file_path: Caminho do arquivo SQL
        dry_run: Se True, apenas mostra o que seria alterado sem modificar
        check_context: Se True, verifica contexto SQL antes de aplicar transformação

    Returns:
        Tupla (número_de_substituições, lista_de_mudanças, número_pulado)
    """
    try:
        content = file_path.read_text(encoding='utf-8')
    except Exception as e:
        print(f"❌ Erro ao ler {file_path}: {e}")
        return 0, [], 0

    original_content = content
    new_content = content
    replacements = 0
    skipped = 0
    changes = []

    # Processar o arquivo inteiro para detectar padrões multi-linha
    for pattern_idx, pattern in enumerate(PATTERNS):
        matches = list(pattern.finditer(new_content))

        for match in reversed(matches):  # Reverso para não afetar índices
            column_name = match.group(1)
            data_type = match.group(2)

            # Encontrar em qual linha começa o match
            before_match = new_content[:match.start()]
            line_num = before_match.count('\n') + 1

            # Extrair o texto original do match (pode ser multi-linha)
            original_text = match.group(0)

            # Extrair a linha completa onde está o match para análise de contexto
            lines = new_content.split('\n')
            sql_line = lines[line_num - 1] if line_num - 1 < len(lines) else original_text

            # Verificar contexto se habilitado
            should_transform = True
            skip_reason = ""

            if check_context:
                should_transform, reason = should_apply_transformation(
                    content,
                    column_name,
                    line_num,
                    sql_line
                )
                skip_reason = reason

            if not should_transform:
                skipped += 1
                if '\n' in original_text:
                    original_compact = ' '.join(original_text.split())
                    changes.append(
                        f"  Linha {line_num} (PULADO - {skip_reason}):\n"
                        f"    {original_compact}"
                    )
                else:
                    changes.append(
                        f"  Linha {line_num} (PULADO - {skip_reason}):\n"
                        f"    {original_text.strip()}"
                    )
                continue

            macro_call = generate_macro_call(pattern_idx, column_name, data_type)

            # Substituir
            new_content = new_content[:match.start()] + macro_call + new_content[match.end():]
            replacements += 1

            # Formatar a mudança para exibição
            # Se for multi-linha, mostrar de forma compacta
            if '\n' in original_text:
                original_compact = ' '.join(original_text.split())
                changes.append(
                    f"  Linha {line_num} (multi-linha):\n"
                    f"    Antes: {original_compact}\n"
                    f"    Depois: {macro_call}"
                )
            else:
                changes.append(
                    f"  Linha {line_num}:\n"
                    f"    Antes: {original_text.strip()}\n"
                    f"    Depois: {macro_call}"
                )

    # Salvar se houver mudanças e não for dry-run
    if replacements > 0 and not dry_run:
        try:
            file_path.write_text(new_content, encoding='utf-8')
        except Exception as e:
            print(f"❌ Erro ao salvar {file_path}: {e}")
            return 0, [], 0

    return replacements, changes, skipped


def find_sql_files(base_path: Path) -> List[Path]:
    """Encontra todos os arquivos .sql recursivamente."""
    return list(base_path.rglob('*.sql'))


def main():
    parser = argparse.ArgumentParser(
        description='Substitui padrões SAFE_CAST(REGEXP_REPLACE...) pelo macro clean_and_cast',
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
        help='Mostra todas as substituições feitas'
    )

    parser.add_argument(
        '--no-context-check',
        action='store_true',
        help='Desabilita verificação de contexto SQL (aplica transformação em todos os casos)'
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

    check_context = not args.no_context_check
    if check_context:
        print("🧠 Análise de contexto SQL habilitada (transformações apenas em sources/tabelas hardcoded)")
        print("   Use --no-context-check para desabilitar\n")
    else:
        print("⚠️  Análise de contexto desabilitada - transformações serão aplicadas em todos os casos\n")

    total_replacements = 0
    total_skipped = 0
    files_modified = 0

    for sql_file in sql_files:
        replacements, changes, skipped = process_file(sql_file, dry_run=args.dry_run, check_context=check_context)

        if replacements > 0 or skipped > 0:
            if replacements > 0:
                files_modified += 1
            total_replacements += replacements
            total_skipped += skipped

            rel_path = sql_file.relative_to(script_dir)
            print(f"\n{'🔍' if args.dry_run else '✅'} {rel_path}")
            if replacements > 0 and skipped > 0:
                print(f"  {replacements} substituição(ões), {skipped} pulado(s)")
            elif replacements > 0:
                print(f"  {replacements} substituição(ões)")
            else:
                print(f"  {skipped} pulado(s)")

            if args.verbose or args.dry_run:
                for change in changes:
                    print(change)

    # Resumo
    print("\n" + "="*80)
    print(f"📊 RESUMO:")
    print(f"  Arquivos processados: {len(sql_files)}")
    print(f"  Arquivos modificados: {files_modified}")
    print(f"  Total de substituições: {total_replacements}")
    if check_context and total_skipped > 0:
        print(f"  Total pulado (já transformado via ref): {total_skipped}")

    if args.dry_run and total_replacements > 0:
        print(f"\n💡 Execute sem --dry-run para aplicar as mudanças")
    elif total_replacements > 0:
        print(f"\n✅ Substituições aplicadas com sucesso!")
    else:
        print(f"\n✨ Nenhuma substituição necessária")


if __name__ == '__main__':
    main()
