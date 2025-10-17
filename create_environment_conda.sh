#!/bin/bash
set -euo pipefail

ENV_NAME="dynamic-index"
REINSTALL=false
if [[ "${1:-}" == "--reinstall" ]]; then REINSTALL=true; fi

if ! command -v conda >/dev/null 2>&1; then
  echo "Error: Conda not found"; exit 1
fi

if conda env list | grep -qE "^\s*${ENV_NAME}\s"; then
  if $REINSTALL; then
    conda remove -y -n "$ENV_NAME" --all
  else
    echo "Env exists. Use --reinstall."
    exit 0
  fi
fi

echo "Creating conda env (latest Python)…"
conda create -y -n "$ENV_NAME" python

echo "Installing packages…"
conda run -n "$ENV_NAME" conda install -y -c conda-forge \
  "duckdb>=1.4.1" numpy pyarrow

echo "Verifying…"
conda run --no-capture-output -n "$ENV_NAME" python - <<'EOF'
import duckdb, numpy, pyarrow
print("duckdb:", duckdb.__version__)
print("numpy:", numpy.__version__)
print("pyarrow:", pyarrow.__version__)
assert tuple(int(x) for x in duckdb.__version__.split('.')[:3]) >= (1,4,1)
print("All required packages imported successfully (duckdb >= 1.4.1).")
EOF

# If CLI was installed, you can also check:
# conda run --no-capture-output -n "$ENV_NAME" duckdb --version

echo "Creating project directories…"
# Creates:
# - ducklake/
# - ducklake/data_files/
# - parquet/
# - parquet/index/
# - parquet/webcrawl_data/
mkdir -p \
  "ducklake/data_files" \
  "parquet/index" \
  "parquet/webcrawl_data"

echo "Environment '$ENV_NAME' ready. Activate: conda activate $ENV_NAME"
echo "Created directories:"
echo "  ./ducklake/data_files"
echo "  ./parquet/index"
echo "  ./parquet/webcrawl_data"