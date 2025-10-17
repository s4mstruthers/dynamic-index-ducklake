#!/bin/bash
# Script: create_dynamic_index_ducklake_env.sh
# Purpose: Create a Conda environment for DuckDB-based data workflows
# Author: Sam Struthers
# Description:
#   - Creates a Conda environment with the latest Python and installs:
#       duckdb>=1.4.1, numpy, pyarrow
#   - Verifies package installation
#   - Optionally reinstalls if --reinstall is passed
#   - Sets up project directory structure:
#       ducklake/data_files
#       parquet/index
#       parquet/webcrawl_data

set -euo pipefail

# --- Configuration ---
ENV_NAME="dynamic-index-ducklake"
REINSTALL=false
if [[ "${1:-}" == "--reinstall" ]]; then
  REINSTALL=true
fi

# --- Check for Conda ---
if ! command -v conda >/dev/null 2>&1; then
  echo "Error: Conda not found in PATH."
  echo "Please install Miniconda or Anaconda before running this script."
  exit 1
fi

# --- Remove existing environment if requested ---
if conda env list | grep -qE "^\s*${ENV_NAME}\s"; then
  if $REINSTALL; then
    echo "Existing environment detected. Reinstalling '$ENV_NAME'..."
    conda remove -y -n "$ENV_NAME" --all
  else
    echo "Environment '$ENV_NAME' already exists. Use '--reinstall' to replace it."
    exit 0
  fi
fi

# --- Create Conda environment with latest Python ---
echo "Creating Conda environment: $ENV_NAME (latest Python)..."
conda create -y -n "$ENV_NAME" python

# --- Install required packages from conda-forge ---
echo "Installing dependencies..."
conda run -n "$ENV_NAME" conda install -y -c conda-forge \
  "duckdb>=1.4.1" numpy pyarrow

# --- Verify installation and version requirements ---
echo "Verifying package installation..."
conda run --no-capture-output -n "$ENV_NAME" python - <<'EOF'
import duckdb, numpy, pyarrow
print(f"duckdb: {duckdb.__version__}")
print(f"numpy: {numpy.__version__}")
print(f"pyarrow: {pyarrow.__version__}")
assert tuple(int(x) for x in duckdb.__version__.split('.')[:3]) >= (1,4,1)
print("All required packages imported successfully (duckdb >= 1.4.1).")
EOF

# --- Create project directory structure ---
echo "Creating project directories..."
mkdir -p \
  "ducklake/data_files" \
  "parquet/index" \
  "parquet/webcrawl_data"

# --- Completion message ---
echo "Environment '$ENV_NAME' created successfully."
echo "Activate it with: conda activate $ENV_NAME"
echo "Directories created:"
echo "  ducklake/data_files"
echo "  parquet/index"
echo "  parquet/webcrawl_data"