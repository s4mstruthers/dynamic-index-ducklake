#!/bin/bash
# Script: setup.sh
# Purpose: Create a Conda environment and the complete project folder structure
# Author: Sam Struthers

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
    # We continue here to ensure folders are created even if env exists
  fi
fi

# --- Create Conda environment (only if it doesn't exist) ---
if ! conda env list | grep -qE "^\s*${ENV_NAME}\s"; then
    echo "Creating Conda environment: $ENV_NAME (latest Python)..."
    conda create -y -n "$ENV_NAME" python
fi

# --- Install required packages ---
echo "Installing dependencies..."
# Using --no-update-deps to speed up if already installed
conda run -n "$ENV_NAME" conda install -y -c conda-forge \
  "duckdb>=1.4.1" numpy pyarrow matplotlib pandas

# --- Verify installation ---
echo "Verifying package installation..."
conda run --no-capture-output -n "$ENV_NAME" python - <<'EOF'
import duckdb, numpy, pyarrow, pandas, matplotlib
print(f"duckdb: {duckdb.__version__}")
assert tuple(int(x) for x in duckdb.__version__.split('.')[:3]) >= (1,4,1)
print("All required packages (duckdb, numpy, pyarrow, pandas, matplotlib) imported successfully.")
EOF

# --- Create COMPLETE Project Directory Structure ---
echo "Creating project directories..."

# 1. DuckLake managed storage
mkdir -p "ducklake/data_files"

# 2. Parquet artifacts
# backup_parquets is required for the 'reset' command in dynamic_index.py
mkdir -p "parquet/index"
mkdir -p "parquet/webcrawl_data"
mkdir -p "parquet/backup_parquets"

# 3. Results storage
# Required for performance testing output
mkdir -p "results/performance_results"
mkdir -p "results/performance_plots"
mkdir -p "results/query_terms"

# --- Completion message ---
echo "------------------------------------------------"
echo "Environment '$ENV_NAME' setup complete."
echo "Activate it with: conda activate $ENV_NAME"
echo ""
echo "Created Directory Structure:"
echo "  ├── ducklake/data_files/"
echo "  ├── parquet/"
echo "  │   ├── backup_parquets/"
echo "  │   ├── index/"
echo "  │   └── webcrawl_data/"
echo "  └── results/"
echo "      ├── performance_plots/"
echo "      ├── performance_results/"
echo "      └── query_terms/"
echo "------------------------------------------------"