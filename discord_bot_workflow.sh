#!/bin/bash
# Enhanced Discord bot launcher script with better resilience
# This script will run the restart_handler.sh which provides ultimate stability

echo "===================================================="
echo "  Starting Discord Bot with Ultimate Stability"
echo "===================================================="
echo "  $(date)"
echo "===================================================="

# Make sure our scripts are executable
chmod +x bot_wrapper.py restart_handler.sh

# Run the restart handler
./restart_handler.sh