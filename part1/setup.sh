#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
python3.12 -m venv "${VENV_DIR:-.venv}"
"${VENV_DIR:-.venv}/bin/python" -m pip install -r requirements.txt
"${VENV_DIR:-.venv}/bin/python" -m playwright install chromium
