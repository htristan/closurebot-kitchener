#!/bin/bash

# Activate the virtual environment
source venv/bin/activate

# Load environment variables
source .env_vars

# Run the Python script
python3 scrape.py