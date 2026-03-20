#!/usr/bin/env python3
"""
DBT Scripts CLI - Ponto de entrada principal.

Uso:
    python cli.py <comando> [opções]
    python cli.py --help
"""

import sys
from pathlib import Path

# Adicionar diretório scripts ao path
SCRIPTS_DIR = Path(__file__).parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

# Importar e executar CLI principal
from scripts.cli.main import main

if __name__ == "__main__":
    sys.exit(main())
