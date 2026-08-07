#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- Step 1: recover the authoritative sample stream (#OBS-6170) ------------
# The rollout left /app/data/events.json truncated. Merge the pre-rollout
# snapshot with the replay journal and write the result back to that path;
# nothing the reconciler emits is correct until this is done.

python3 "${SCRIPT_DIR}/recover_stream.py"

# --- Step 2: restore the reconciler and produce the review artifacts --------

cp "${SCRIPT_DIR}/export_report_fixed.py" /app/workflow/export_report.py
python3 /app/workflow/export_report.py --output-dir /app/output
