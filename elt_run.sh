#!/bin/bash

echo "========== Start Orcestration Process =========="

# Virtual Environment Path
VENV_PATH="/home/ibnufajar/OLIST-ETL-PROJECT/.venv/bin/activate"

# Activate Virtual Environment
source "$VENV_PATH"

# Set Python script
PYTHON_SCRIPT="/home/ibnufajar/OLIST-ETL-PROJECT/elt_main.py"

# Run Python Script 
python3 "$PYTHON_SCRIPT"


echo "========== End of Orcestration Process =========="