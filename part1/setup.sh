#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
python3.12 -m venv "${VENV_DIR:-.venv}"
requirements_file=requirements.txt
if [[ -f requirements.lock.txt ]]; then
  requirements_file=requirements.lock.txt
fi
"${VENV_DIR:-.venv}/bin/python" -m pip install -r "$requirements_file"
"${VENV_DIR:-.venv}/bin/python" -m playwright install chromium
