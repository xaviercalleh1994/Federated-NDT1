#!/bin/bash
# setup_venv.sh — crea el venv e instala dependencias
# Uso: bash setup_venv.sh

set -euo pipefail

VENV_DIR="${1:-venv}"

echo "Creating venv in ${VENV_DIR}..."
python3 -m venv "${VENV_DIR}"

echo "Activating venv..."
source "${VENV_DIR}/bin/activate"

echo "Installing dependencies..."
pip install --upgrade pip

# DTM y ONM
pip install orjson

# Análisis (experiment_1.py)
pip install numpy pandas matplotlib scipy tabulate

echo ""
echo "Done. To activate:"
echo "  source ${VENV_DIR}/bin/activate"
echo ""
echo "To run DTM:"
echo "  source ${VENV_DIR}/bin/activate && python3 dtm.py --help"