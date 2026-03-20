#!/usr/bin/env python3
"""
DBT Scripts CLI - Ferramenta centralizada para executar scripts de manutenção do projeto dbt.

Este CLI unifica vários scripts de manutenção e transformação de código,
fornecendo uma interface consistente e segura (dry-run por padrão).

Uso:
    python scripts/main.py <comando> [opções]
    python scripts/main.py <comando> --help  # Ajuda específica do comando

Comandos disponíveis:
    clean-cast      Substitui SAFE_CAST(REGEXP_REPLACE...) pelo macro clean_and_cast
    remove-quotes   Remove quote: true de campos não-string em arquivos YAML
    fix-exemplo     Substitui "Ex:" por "Exemplo" em arquivos YAML
    list            Lista todos os scripts disponíveis

Flags globais:
    --apply         Executa as mudanças (desativa dry-run)
    --dry-run       Mostra o que seria alterado sem modificar (padrão)
    --path PATH     Caminho base para processar (padrão: models)
    --verbose, -v   Mostra detalhes de todas as alterações
    --help, -h      Mostra esta ajuda

Exemplos:
    # Ver o que seria alterado (dry-run padrão)
    python scripts/main.py clean-cast

    # Aplicar mudanças em uma pasta específica
    python scripts/main.py clean-cast --apply --path models/raw/sma

    # Ver ajuda de um comando específico
    python scripts/main.py clean-cast --help

    # Listar todos os scripts
    python scripts/main.py list
"""

import argparse
import sys
from pathlib import Path
from typing import List, Tuple, Dict, Any
import importlib.util
import textwrap


# Diretório base do projeto
PROJECT_ROOT = Path(__file__).parent.parent
SCRIPTS_DIR = PROJECT_ROOT / "scripts"


class ScriptRegistry:
    """Registro de scripts disponíveis e suas configurações."""

    SCRIPTS = {
        "clean-cast": {
            "file": "replace_with_clean_and_cast_macro.py",
            "description": "Substitui SAFE_CAST(REGEXP_REPLACE...) pelo macro clean_and_cast",
            "long_description": r"""
                Detecta padrões como SAFE_CAST(REGEXP_REPLACE(coluna, r'\.0$', '') AS tipo)
                e substitui pelo macro {{ clean_and_cast('coluna', 'tipo') }}.

                Suporta 6 variações do padrão, incluindo multi-linha.
                Ideal para padronizar transformações de tipo em arquivos SQL.
            """,
            "default_path": "models",
            "supports": ["--path", "--verbose", "--apply"],
            "examples": [
                "python scripts/main.py clean-cast --dry-run",
                "python scripts/main.py clean-cast --apply --path models/raw/sma",
            ]
        },
        "remove-quotes": {
            "file": "remove_nonstring_quotes_all.py",
            "description": "Remove quote: true de campos não-string em arquivos YAML",
            "long_description": """
                Remove a propriedade 'quote: true' de colunas em schemas YAML
                que não são do tipo string. Isso evita erros de sintaxe no dbt.

                Processa apenas arquivos .yml em models/.
            """,
            "default_path": "models",
            "supports": ["--path", "--apply"],
            "examples": [
                "python scripts/main.py remove-quotes --dry-run",
                "python scripts/main.py remove-quotes --apply --path models/raw/pgm",
            ]
        },
        "fix-exemplo": {
            "file": "replace_ex_with_exemplo.py",
            "description": "Substitui 'Ex:' por 'Exemplo' em arquivos YAML",
            "long_description": """
                Padroniza descrições em schemas YAML substituindo
                abreviações como 'Ex:' ou 'Ex.:' por 'Exemplo '.

                Processa apenas arquivos .yml em models/.
            """,
            "default_path": "models",
            "supports": ["--path", "--apply"],
            "examples": [
                "python scripts/main.py fix-exemplo --dry-run",
                "python scripts/main.py fix-exemplo --apply",
            ]
        },
    }

    @classmethod
    def get_script_info(cls, name: str) -> Dict[str, Any]:
        """Retorna informações sobre um script."""
        return cls.SCRIPTS.get(name, {})

    @classmethod
    def list_scripts(cls) -> List[str]:
        """Lista todos os scripts disponíveis."""
        return list(cls.SCRIPTS.keys())

    @classmethod
    def script_exists(cls, name: str) -> bool:
        """Verifica se um script existe."""
        return name in cls.SCRIPTS


def print_banner():
    """Imprime banner do CLI."""
    banner = """
    ╔════════════════════════════════════════════════╗
    ║         DBT Scripts CLI - queries-rj           ║
    ║   Ferramentas de manutenção e transformação    ║
    ╚════════════════════════════════════════════════╝
    """
    print(banner)


def print_script_list():
    """Imprime lista formatada de scripts disponíveis."""
    print("\n📋 Scripts Disponíveis:\n")

    for name, info in ScriptRegistry.SCRIPTS.items():
        print(f"  {name:15} - {info['description']}")

    print("\nUse 'python scripts/main.py <comando> --help' para mais informações.\n")


def create_parser() -> argparse.ArgumentParser:
    """Cria o parser de argumentos principal."""

    parser = argparse.ArgumentParser(
        description="DBT Scripts CLI - Ferramenta centralizada para scripts de manutenção",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""
            Exemplos de uso:
              python scripts/main.py list
              python scripts/main.py clean-cast --dry-run
              python scripts/main.py clean-cast --apply --path models/raw/sma
              python scripts/main.py remove-quotes --apply --verbose

            Flags globais aplicam-se a todos os comandos.
            Use --help após um comando para opções específicas.
        """)
    )

    subparsers = parser.add_subparsers(
        dest="command",
        title="Comandos",
        description="Scripts disponíveis para execução",
        help="Use '<comando> --help' para ajuda específica"
    )

    # Comando: list
    list_parser = subparsers.add_parser(
        "list",
        help="Lista todos os scripts disponíveis",
        description="Mostra informações sobre todos os scripts disponíveis no CLI"
    )

    # Comando: clean-cast
    clean_cast_parser = subparsers.add_parser(
        "clean-cast",
        help="Substitui SAFE_CAST(REGEXP_REPLACE...) pelo macro",
        description=ScriptRegistry.SCRIPTS["clean-cast"]["long_description"],
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="\n".join(ScriptRegistry.SCRIPTS["clean-cast"]["examples"])
    )
    add_common_args(clean_cast_parser, "clean-cast")

    # Comando: remove-quotes
    remove_quotes_parser = subparsers.add_parser(
        "remove-quotes",
        help="Remove quote: true de campos não-string",
        description=ScriptRegistry.SCRIPTS["remove-quotes"]["long_description"],
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="\n".join(ScriptRegistry.SCRIPTS["remove-quotes"]["examples"])
    )
    add_common_args(remove_quotes_parser, "remove-quotes")

    # Comando: fix-exemplo
    fix_exemplo_parser = subparsers.add_parser(
        "fix-exemplo",
        help="Substitui 'Ex:' por 'Exemplo'",
        description=ScriptRegistry.SCRIPTS["fix-exemplo"]["long_description"],
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="\n".join(ScriptRegistry.SCRIPTS["fix-exemplo"]["examples"])
    )
    add_common_args(fix_exemplo_parser, "fix-exemplo")

    return parser


def add_common_args(parser: argparse.ArgumentParser, script_name: str):
    """Adiciona argumentos comuns a um subparser."""

    script_info = ScriptRegistry.get_script_info(script_name)

    if "--apply" in script_info.get("supports", []):
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Aplica as mudanças (desativa dry-run). CUIDADO: modifica arquivos!"
        )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Mostra o que seria alterado sem modificar (padrão)"
    )

    if "--path" in script_info.get("supports", []):
        parser.add_argument(
            "--path",
            type=str,
            default=script_info.get("default_path", "models"),
            help=f"Caminho base para processar (padrão: {script_info.get('default_path', 'models')})"
        )

    if "--verbose" in script_info.get("supports", []):
        parser.add_argument(
            "--verbose", "-v",
            action="store_true",
            help="Mostra detalhes de todas as alterações"
        )

    # Argumentos adicionais específicos
    parser.add_argument(
        "--file-pattern",
        type=str,
        help="Padrão glob para filtrar arquivos (ex: '*.sql', '*.yml')"
    )


def run_clean_cast(args):
    """Executa o script clean_and_cast."""
    from replace_with_clean_and_cast_macro import main as clean_cast_main
    from replace_with_clean_and_cast_macro import find_sql_files, process_file

    print(f"\n🔧 Executando: clean-cast")
    print(f"📁 Caminho: {args.path}")
    print(f"🔒 Modo: {'DRY-RUN (simulação)' if not args.apply else 'APLICAR MUDANÇAS'}\n")

    base_path = PROJECT_ROOT / args.path

    if not base_path.exists():
        print(f"❌ Caminho não encontrado: {base_path}")
        return 1

    sql_files = find_sql_files(base_path)
    print(f"📄 Encontrados {len(sql_files)} arquivos SQL\n")

    if not args.apply:
        print("⚠️  MODO DRY-RUN - Nenhum arquivo será modificado\n")

    total_replacements = 0
    files_modified = 0

    for sql_file in sql_files:
        replacements, changes = process_file(sql_file, dry_run=not args.apply)

        if replacements > 0:
            files_modified += 1
            total_replacements += replacements

            rel_path = sql_file.relative_to(PROJECT_ROOT)
            print(f"\n{'🔍' if not args.apply else '✅'} {rel_path}")
            print(f"  {replacements} substituição(ões)")

            if args.verbose or not args.apply:
                for change in changes:
                    print(change)

    print("\n" + "="*80)
    print(f"📊 RESUMO:")
    print(f"  Arquivos processados: {len(sql_files)}")
    print(f"  Arquivos modificados: {files_modified}")
    print(f"  Total de substituições: {total_replacements}")

    if not args.apply and total_replacements > 0:
        print(f"\n💡 Execute com --apply para aplicar as mudanças")
    elif total_replacements > 0:
        print(f"\n✅ Substituições aplicadas com sucesso!")
    else:
        print(f"\n✨ Nenhuma substituição necessária")

    return 0


def run_remove_quotes(args):
    """Executa o script remove_nonstring_quotes."""
    import glob
    import os

    print(f"\n🔧 Executando: remove-quotes")
    print(f"📁 Caminho: {args.path}")
    print(f"🔒 Modo: {'DRY-RUN (simulação)' if not args.apply else 'APLICAR MUDANÇAS'}\n")

    base_path = PROJECT_ROOT / args.path

    if not base_path.exists():
        print(f"❌ Caminho não encontrado: {base_path}")
        return 1

    # Buscar arquivos YAML
    pattern = args.file_pattern or "*.yml"
    yml_files = list(base_path.rglob(pattern))

    print(f"📄 Encontrados {len(yml_files)} arquivos YAML\n")

    if not args.apply:
        print("⚠️  MODO DRY-RUN - Nenhum arquivo será modificado\n")

    files_changed = 0

    for yml_file in yml_files:
        try:
            with open(yml_file, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()

            new_lines = []
            changed = False

            for i, line in enumerate(lines):
                if line.strip().startswith('quote:'):
                    j = i - 1
                    while j >= 0 and lines[j].strip() == "":
                        j -= 1
                    if j >= 0 and 'data_type:' in lines[j]:
                        if 'string' not in lines[j]:
                            changed = True
                            if args.verbose or not args.apply:
                                print(f"  Removendo quote: true (linha {i+1}) - tipo não-string")
                            continue
                new_lines.append(line)

            if changed:
                files_changed += 1
                rel_path = yml_file.relative_to(PROJECT_ROOT)
                print(f"{'🔍' if not args.apply else '✅'} {rel_path}")

                if args.apply:
                    with open(yml_file, 'w', encoding='utf-8') as f:
                        f.writelines(new_lines)

        except Exception as e:
            print(f"❌ Erro em {yml_file}: {e}")

    print("\n" + "="*80)
    print(f"📊 RESUMO:")
    print(f"  Arquivos processados: {len(yml_files)}")
    print(f"  Arquivos modificados: {files_changed}")

    if not args.apply and files_changed > 0:
        print(f"\n💡 Execute com --apply para aplicar as mudanças")
    elif files_changed > 0:
        print(f"\n✅ Mudanças aplicadas com sucesso!")
    else:
        print(f"\n✨ Nenhuma mudança necessária")

    return 0


def run_fix_exemplo(args):
    """Executa o script replace_ex_with_exemplo."""
    import re

    print(f"\n🔧 Executando: fix-exemplo")
    print(f"📁 Caminho: {args.path}")
    print(f"🔒 Modo: {'DRY-RUN (simulação)' if not args.apply else 'APLICAR MUDANÇAS'}\n")

    base_path = PROJECT_ROOT / args.path

    if not base_path.exists():
        print(f"❌ Caminho não encontrado: {base_path}")
        return 1

    pattern = re.compile(r"Ex\.?:")

    # Buscar arquivos YAML
    file_pattern = args.file_pattern or "*.yml"
    yml_files = list(base_path.rglob(file_pattern))

    print(f"📄 Encontrados {len(yml_files)} arquivos YAML\n")

    if not args.apply:
        print("⚠️  MODO DRY-RUN - Nenhum arquivo será modificado\n")

    files_changed = 0
    total_replacements = 0

    for yml_file in yml_files:
        try:
            with open(yml_file, 'r', encoding='utf-8') as f:
                text = f.read()

            matches = list(pattern.finditer(text))
            new_text = pattern.sub('Exemplo ', text)

            if new_text != text:
                files_changed += 1
                total_replacements += len(matches)

                rel_path = yml_file.relative_to(PROJECT_ROOT)
                print(f"{'🔍' if not args.apply else '✅'} {rel_path}")
                print(f"  {len(matches)} substituição(ões)")

                if args.apply:
                    with open(yml_file, 'w', encoding='utf-8') as f:
                        f.write(new_text)

        except Exception as e:
            print(f"❌ Erro em {yml_file}: {e}")

    print("\n" + "="*80)
    print(f"📊 RESUMO:")
    print(f"  Arquivos processados: {len(yml_files)}")
    print(f"  Arquivos modificados: {files_changed}")
    print(f"  Total de substituições: {total_replacements}")

    if not args.apply and files_changed > 0:
        print(f"\n💡 Execute com --apply para aplicar as mudanças")
    elif files_changed > 0:
        print(f"\n✅ Mudanças aplicadas com sucesso!")
    else:
        print(f"\n✨ Nenhuma mudança necessária")

    return 0


def main():
    """Função principal do CLI."""

    # Adicionar diretório de scripts ao path
    sys.path.insert(0, str(SCRIPTS_DIR))

    parser = create_parser()

    # Se não houver argumentos, mostrar ajuda
    if len(sys.argv) == 1:
        print_banner()
        print_script_list()
        parser.print_help()
        return 0

    args = parser.parse_args()

    # Comando: list
    if args.command == "list":
        print_banner()
        print_script_list()
        return 0

    # Se --apply foi passado, desabilitar dry-run
    if hasattr(args, 'apply') and args.apply:
        args.dry_run = False

    # Executar comando apropriado
    if args.command == "clean-cast":
        return run_clean_cast(args)
    elif args.command == "remove-quotes":
        return run_remove_quotes(args)
    elif args.command == "fix-exemplo":
        return run_fix_exemplo(args)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
