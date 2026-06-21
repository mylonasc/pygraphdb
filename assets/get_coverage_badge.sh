#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
"${PYTHON:-python3}" "$SCRIPT_DIR/../scripts/update_coverage_badge.py"
