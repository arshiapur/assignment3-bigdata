#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
python3.12 -m venv "${VENV_DIR:-.venv}"
if [[ -f requirements.lock.txt ]]; then
  "${VENV_DIR:-.venv}/bin/python" -m pip install -r requirements.lock.txt
else
  "${VENV_DIR:-.venv}/bin/python" -m pip install -r requirements-validation.txt
fi
"${VENV_DIR:-.venv}/bin/python" -m pip check
