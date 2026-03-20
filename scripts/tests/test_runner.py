#!/usr/bin/env python3
"""
Test Runner para validar transformações SQL.

Compara arquivos de input com expected após aplicar transformações.
"""

import sys
from pathlib import Path
from typing import List, Tuple, Dict
import difflib
import tempfile
import shutil

# Adicionar paths necessários
SCRIPTS_DIR = Path(__file__).parent.parent
PROJECT_ROOT = SCRIPTS_DIR.parent
sys.path.insert(0, str(SCRIPTS_DIR))

from core.replace_with_clean_and_cast_macro import process_file as process_clean_cast
from core.enforce_id_string_type import find_id_columns_in_sql, fix_sql_file

# Diretórios de teste
FIXTURES_DIR = Path(__file__).parent / "fixtures"
INPUT_DIR = FIXTURES_DIR / "input"
EXPECTED_DIR = FIXTURES_DIR / "expected"


class TestResult:
    """Resultado de um teste."""

    def __init__(self, name: str, passed: bool, message: str = ""):
        self.name = name
        self.passed = passed
        self.message = message

    def __str__(self):
        status = "✅ PASS" if self.passed else "❌ FAIL"
        msg = f"\n   {self.message}" if self.message else ""
        return f"{status} - {self.name}{msg}"


def normalize_whitespace(content: str) -> str:
    """Normaliza espaços em branco para comparação."""
    lines = content.split('\n')
    normalized = []
    for line in lines:
        # Remove trailing whitespace, mas mantém leading
        normalized.append(line.rstrip())
    return '\n'.join(normalized)


def run_clean_cast_test(input_file: Path, expected_file: Path) -> TestResult:
    """
    Testa transformação clean_and_cast.

    Args:
        input_file: Arquivo de entrada
        expected_file: Resultado esperado

    Returns:
        TestResult com resultado do teste
    """
    # Criar cópia temporária do input
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as tmp:
        tmp.write(input_file.read_text())
        tmp_path = Path(tmp.name)

    try:
        # Aplicar transformação
        replacements, changes, skipped = process_clean_cast(
            tmp_path,
            dry_run=False,  # Aplicar de verdade no arquivo temp
            check_context=True
        )

        # Ler resultado
        result_content = tmp_path.read_text()
        expected_content = expected_file.read_text()

        # Normalizar espaços
        result_norm = normalize_whitespace(result_content)
        expected_norm = normalize_whitespace(expected_content)

        # Comparar
        if result_norm == expected_norm:
            return TestResult(
                input_file.name,
                True,
                f"Transformações: {replacements}, Pulados: {skipped}"
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
                False,
                f"Output differs from expected:\n{diff[:500]}"
            )

    finally:
        # Limpar arquivo temporário
        tmp_path.unlink()


def run_enforce_id_test(input_file: Path, expected_file: Path) -> TestResult:
    """
    Testa transformação enforce-id-string.

    Args:
        input_file: Arquivo de entrada
        expected_file: Resultado esperado

    Returns:
        TestResult com resultado do teste
    """
    # Criar cópia temporária do input
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as tmp:
        tmp.write(input_file.read_text())
        tmp_path = Path(tmp.name)

    try:
        # Detectar issues
        issues, skipped = find_id_columns_in_sql(tmp_path, check_context=True)

        # Aplicar correções
        if issues:
            fix_sql_file(tmp_path, issues, dry_run=False)

        # Ler resultado
        result_content = tmp_path.read_text()
        expected_content = expected_file.read_text()

        # Normalizar espaços
        result_norm = normalize_whitespace(result_content)
        expected_norm = normalize_whitespace(expected_content)

        # Comparar
        if result_norm == expected_norm:
            return TestResult(
                input_file.name,
                True,
                f"Issues corrigidos: {len(issues)}, Pulados: {len(skipped)}"
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
                False,
                f"Output differs from expected:\n{diff[:500]}"
            )

    finally:
        # Limpar arquivo temporário
        tmp_path.unlink()


def run_all_tests() -> List[TestResult]:
    """
    Executa todos os testes de fixtures.

    Returns:
        Lista de TestResult
    """
    results = []

    # Listar todos os arquivos de input
    input_files = sorted(INPUT_DIR.glob("*.sql"))

    print(f"\n{'='*80}")
    print(f"🧪 EXECUTANDO TESTES DE TRANSFORMAÇÃO")
    print(f"{'='*80}\n")
    print(f"Fixtures encontrados: {len(input_files)}\n")

    for input_file in input_files:
        expected_file = EXPECTED_DIR / input_file.name

        if not expected_file.exists():
            results.append(TestResult(
                input_file.name,
                False,
                f"Expected file not found: {expected_file}"
            ))
            continue

        # Determinar qual teste rodar baseado no nome do arquivo
        if "id_columns" in input_file.name:
            result = run_enforce_id_test(input_file, expected_file)
        else:
            result = run_clean_cast_test(input_file, expected_file)

        results.append(result)
        print(result)

    return results


def print_summary(results: List[TestResult]):
    """Imprime resumo dos testes."""
    total = len(results)
    passed = sum(1 for r in results if r.passed)
    failed = total - passed

    print(f"\n{'='*80}")
    print(f"📊 RESUMO DOS TESTES")
    print(f"{'='*80}")
    print(f"Total:   {total}")
    print(f"✅ Passou: {passed}")
    print(f"❌ Falhou: {failed}")

    if failed > 0:
        print(f"\n⚠️  Testes que falharam:")
        for r in results:
            if not r.passed:
                print(f"   - {r.name}")

    print(f"{'='*80}\n")

    return failed == 0


def main():
    """Main function."""

    if not INPUT_DIR.exists() or not EXPECTED_DIR.exists():
        print("❌ Diretórios de fixtures não encontrados!")
        print(f"   Input:    {INPUT_DIR}")
        print(f"   Expected: {EXPECTED_DIR}")
        return 1

    results = run_all_tests()
    success = print_summary(results)

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
