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

# ============================================================
# stop_ndts.sh — stop NDT containers and clean up everything
#
# Usage:
#   ./stop_ndts.sh [LOG_DEST]
#
# With LOG_DEST: archives logs before cleaning
# Without:      just stops and cleans
# ============================================================

set -euo pipefail

LOG_DEST=${1:-""}
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PREFIX="exp1-ndt"
LOG_DIR="${SCRIPT_DIR}/logs/current"

# ── Archive logs if destination given ────────────────────────
if [ -n "${LOG_DEST}" ]; then
    mkdir -p "${LOG_DEST}"
    if ls "${LOG_DIR}"/*.log > /dev/null 2>&1; then
        cp "${LOG_DIR}"/*.log "${LOG_DEST}/"
        echo "Logs saved to ${LOG_DEST}"
    fi
fi

# ── Stop and remove all NDT containers ───────────────────────
echo "Stopping containers..."
FOUND=0
for container in $(docker ps -a --format '{{.Names}}' 2>/dev/null | grep "^${PREFIX}-" || true); do
    docker rm -f "${container}" > /dev/null 2>&1 && \
        echo "  Removed ${container}" || true
    FOUND=1
done
[ "${FOUND}" -eq 0 ] && echo "  No containers found"

# ── Clean log directory ──────────────────────────────────────
echo "Cleaning logs..."
rm -f "${LOG_DIR}"/ndt_*.log
echo "  logs/current/ cleaned"

# ── Clean Mininet residual state ─────────────────────────────
echo "Cleaning Mininet state..."
sudo mn -c > /dev/null 2>&1 && echo "  mn -c done" || true

echo "Done. Ready for next run."