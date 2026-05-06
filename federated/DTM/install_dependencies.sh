#!/bin/bash

# Copyright (C) 2026 Xavier Calle-Heredia, Xavier Hesselbach
# Universitat Politècnica de Catalunya (UPC)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
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