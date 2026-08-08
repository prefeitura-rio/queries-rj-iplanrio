#!/usr/bin/env python3
"""
Test Runner V2 - Testa comandos da CLI usando a interface real.

Organização:
  tests/
    ├── clean-cast/          # Testes do comando clean-cast
    │   ├── input/
    │   └── expected/
    ├── enforce-id-string/   # Testes do comando enforce-id-string
    │   ├── input/
    │   └── expected/
    └── integration/         # Testes de integração
        ├── input/
        └── expected/

Cada teste:
  1. Copia arquivo de input/ para temp
  2. Executa CLI real: python cli.py <comando> --apply <temp_file>
  3. Compara resultado com expected/
"""

import sys
import subprocess
from pathlib import Path
from typing import List, Tuple, Dict
import difflib
import tempfile
import shutil

# Diretórios
PROJECT_ROOT = Path(__file__).parent.parent.parent
TESTS_DIR = Path(__file__).parent
CLI_PATH = PROJECT_ROOT / "cli.py"


class TestResult:
    """Resultado de um teste."""

    def __init__(self, name: str, command: str, passed: bool, message: str = ""):
        self.name = name
        self.command = command
        self.passed = passed
        self.message = message

    def __str__(self):
        status = "✅ PASS" if self.passed else "❌ FAIL"
        cmd_badge = f"[{self.command}]"
        msg = f"\n   {self.message}" if self.message else ""
        return f"{status} {cmd_badge:20} {self.name}{msg}"


def normalize_whitespace(content: str) -> str:
    """Normaliza espaços em branco para comparação."""
    lines = content.split('\n')
    normalized = []
    for line in lines:
        normalized.append(line.rstrip())
    return '\n'.join(normalized)


def run_cli_command(command: str, file_path: Path, check_context: bool = True) -> subprocess.CompletedProcess:
    """
    Executa comando da CLI em um arquivo.

    Args:
        command: Nome do comando (clean-cast, enforce-id-string, etc.)
        file_path: Caminho do arquivo SQL a processar
        check_context: Se deve usar análise de contexto

    Returns:
        CompletedProcess com resultado da execução
    """
    cmd = [
        sys.executable,
        str(CLI_PATH),
        command,
        "--apply",
        "--path", str(file_path.parent)
    ]

    # Adicionar flags específicas por comando
    if not check_context:
        cmd.append("--no-context-check")

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT
    )

    return result


def run_test_for_command(
    command: str,
    input_file: Path,
    expected_file: Path,
    check_context: bool = True
) -> TestResult:
    """
    Executa um teste para um comando específico da CLI.

    Args:
        command: Nome do comando (clean-cast, enforce-id-string)
        input_file: Arquivo de entrada
        expected_file: Resultado esperado
        check_context: Se deve usar análise de contexto

    Returns:
        TestResult com resultado do teste
    """
    # Criar diretório temporário dentro de tests/ para manter contexto do projeto
    tmp_dir = TESTS_DIR / f".tmp_{command}"
    tmp_dir.mkdir(exist_ok=True)

    try:
        # Copiar arquivo de input para temp
        test_file = tmp_dir / input_file.name
        shutil.copy2(input_file, test_file)

        # Executar CLI
        result = run_cli_command(command, test_file, check_context)

        # Validar código de retorno
        if result.returncode != 0:
            err = (result.stderr or result.stdout or "").strip()
            return TestResult(
                input_file.name,
                command,
                False,
                f"CLI retornou código {result.returncode}: {err[:500]}"
            )

        # Ler resultado
        result_content = test_file.read_text()
        expected_content = expected_file.read_text()

        # Normalizar espaços
        result_norm = normalize_whitespace(result_content)
        expected_norm = normalize_whitespace(expected_content)

        # Comparar
        if result_norm == expected_norm:
            return TestResult(
                input_file.name,
                command,
                True,
                "Output matches expected"
            )
        else:
            # Gerar diff
            diff = '\n'.join(difflib.unified_diff(
                expected_norm.splitlines(keepends=True),
                result_norm.splitlines(keepends=True),
                fromfile='expected',
                tofile='result',
                lineterm=''
            ))

            return TestResult(
                input_file.name,
                command,
                False,
                f"Output differs:\n{diff[:500]}"
            )

    finally:
        # Limpar arquivo temporário
        if test_file.exists():
            test_file.unlink()


def discover_tests(filter_pattern: str = None) -> Dict[str, List[Tuple[Path, Path]]]:
    """
    Descobre automaticamente os testes organizados por comando.

    Args:
        filter_pattern: Padrão opcional para filtrar testes

    Returns:
        Dict mapeando comando -> lista de (input_file, expected_file)
    """
    tests_by_command = {}

    # Procurar diretórios de teste (clean-cast, enforce-id-string, etc.)
    for command_dir in TESTS_DIR.iterdir():
        if not command_dir.is_dir():
            continue

        command_name = command_dir.name

        # Pular diretórios especiais
        if command_name in ['__pycache__', 'fixtures']:
            continue

        input_dir = command_dir / "input"
        expected_dir = command_dir / "expected"

        if not input_dir.exists() or not expected_dir.exists():
            continue

        # Listar arquivos de input
        input_files = sorted(input_dir.glob("*.sql"))

        # Aplicar filtro se fornecido
        if filter_pattern:
            input_files = [f for f in input_files if filter_pattern.lower() in f.name.lower()]

        # Parear com expected
        test_pairs = []
        for input_file in input_files:
            expected_file = expected_dir / input_file.name
            if expected_file.exists():
                test_pairs.append((input_file, expected_file))

        if test_pairs:
            tests_by_command[command_name] = test_pairs

    return tests_by_command


def run_all_tests(filter_pattern: str = None, verbose: bool = False) -> List[TestResult]:
    """
    Executa todos os testes descobertos automaticamente.

    Args:
        filter_pattern: Padrão opcional para filtrar testes
        verbose: Se True, mostra detalhes de cada teste

    Returns:
        Lista de TestResult
    """
    results = []

    # Descobrir testes
    tests_by_command = discover_tests(filter_pattern)

    print(f"\n{'='*80}")
    print(f"🧪 EXECUTANDO TESTES DA CLI")
    print(f"{'='*80}\n")

    if filter_pattern:
        print(f"Filtro aplicado: '{filter_pattern}'")

    total_tests = sum(len(pairs) for pairs in tests_by_command.values())
    print(f"Comandos encontrados: {len(tests_by_command)}")
    print(f"Total de testes: {total_tests}\n")

    # Executar testes por comando
    for command, test_pairs in sorted(tests_by_command.items()):
        print(f"\n{'─'*80}")
        print(f"🔧 Testando comando: {command}")
        print(f"{'─'*80}\n")

        for input_file, expected_file in test_pairs:
            result = run_test_for_command(command, input_file, expected_file)
            results.append(result)
            print(result)

            # Mostrar detalhes extras em modo verbose
            if verbose and result.message:
                print(f"   Detalhes: {result.message}")

    return results


def print_summary(results: List[TestResult]) -> bool:
    """
    Imprime resumo dos testes por comando.

    Args:
        results: Lista de TestResult

    Returns:
        True se todos passaram, False caso contrário
    """
    # Agrupar por comando
    by_command = {}
    for r in results:
        if r.command not in by_command:
            by_command[r.command] = []
        by_command[r.command].append(r)

    print(f"\n{'='*80}")
    print(f"📊 RESUMO DOS TESTES")
    print(f"{'='*80}\n")

    total = len(results)
    passed = sum(1 for r in results if r.passed)
    failed = total - passed

    # Resumo por comando
    for command in sorted(by_command.keys()):
        cmd_results = by_command[command]
        cmd_passed = sum(1 for r in cmd_results if r.passed)
        cmd_total = len(cmd_results)
        status = "✅" if cmd_passed == cmd_total else "❌"

        print(f"{status} {command:20} - {cmd_passed}/{cmd_total} testes passaram")

    print(f"\n{'─'*80}")
    print(f"TOTAL:   {total}")
    print(f"✅ Passou: {passed}")
    print(f"❌ Falhou: {failed}")

    if failed > 0:
        print(f"\n⚠️  Testes que falharam:")
        for r in results:
            if not r.passed:
                print(f"   [{r.command}] {r.name}")

    print(f"{'='*80}\n")

    return failed == 0


def main():
    """Main function."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Test Runner - Testa comandos da CLI usando interface real"
    )
    parser.add_argument(
        "--filter", "-f",
        type=str,
        help="Filtra testes por nome"
    )
    parser.add_argument(
        "--command", "-c",
        type=str,
        help="Testa apenas um comando específico (clean-cast, enforce-id-string)"
    )

    args = parser.parse_args()

    # Se especificou comando, usar como filtro de diretório
    filter_pattern = args.filter

    results = run_all_tests(filter_pattern)
    success = print_summary(results)

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
