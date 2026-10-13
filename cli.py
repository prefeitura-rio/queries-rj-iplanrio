#!/usr/bin/env python3
"""
DBT Scripts CLI - Ponto de entrada principal.

Uso:
    python cli.py <comando> [opções]
    python cli.py --help
"""

import sys
from pathlib import Path

# Importar e executar CLI principal
from scripts.cli.main import main

if __name__ == "__main__":
    sys.exit(main())
