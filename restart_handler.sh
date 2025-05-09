#!/bin/bash
# Ultimate restart handler for the Discord bot
# This script ensures that the wrapper script itself is running, 
# providing an extra layer of protection

echo "====================================================="
echo "  Ultimate Discord Bot Restart Handler"
echo "====================================================="
echo "  Starting at $(date)"
echo "====================================================="

# Make scripts executable
chmod +x bot_wrapper.py discord_bot_workflow.sh

# Track number of restart attempts
restart_count=0
max_restarts=5
cooldown_period=300 # 5 minutes in seconds

while true; do
    # Check if our main wrapper is running
    if ! pgrep -f "python bot_wrapper.py" > /dev/null; then
        restart_count=$((restart_count + 1))
        echo "$(date) - Bot wrapper not running (restart attempt $restart_count)" | tee -a restart_handler.log
        
        # Check if we need a cooldown period
        if [ $restart_count -gt $max_restarts ]; then
            echo "$(date) - Too many restart attempts ($restart_count), entering cooldown period of $cooldown_period seconds" | tee -a restart_handler.log
            sleep $cooldown_period
            restart_count=0 # Reset counter after cooldown
        fi
        
        # Start the bot wrapper
        echo "$(date) - Starting bot wrapper" | tee -a restart_handler.log
        python bot_wrapper.py &
    else
        # If bot is running, reset counter
        if [ $restart_count -gt 0 ]; then
            echo "$(date) - Bot wrapper seems to be stable now, resetting restart counter" | tee -a restart_handler.log
            restart_count=0
        fi
    fi
    
    # Check every 30 seconds
    sleep 30
done