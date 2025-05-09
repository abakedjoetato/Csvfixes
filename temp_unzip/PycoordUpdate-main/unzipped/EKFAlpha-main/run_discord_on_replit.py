#!/usr/bin/env python3
"""
Simple launcher for the Tower of Temptation PvP Statistics Discord Bot on Replit.
This is the entry point when the Replit Run button is pressed.
"""
import os
import sys
import logging
import asyncio
import subprocess
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bot.log')
    ]
)
logger = logging.getLogger(__name__)

def run_bot():
    """Run the Discord bot."""
    logger.info(f"Starting Discord bot at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Create a flag file to indicate we're running in a workflow
    with open(".running_in_workflow", "w") as f:
        f.write(f"Started at {datetime.now()}")
    
    try:
        # Run the bot.py script
        logger.info("Launching bot via bot.py...")
        result = subprocess.run([sys.executable, "bot.py"], 
                              check=True, 
                              text=True, 
                              capture_output=False)
        
        logger.info(f"Bot process exited with code {result.returncode}")
        return result.returncode
    except subprocess.CalledProcessError as e:
        logger.error(f"Error launching bot: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error running bot: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    # Print a banner to make it clear the bot is starting
    print("=" * 60)
    print("  Tower of Temptation PvP Statistics Discord Bot Launcher")
    print("=" * 60)
    print(f"  Starting bot at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("  Press Ctrl+C to stop the bot")
    print("=" * 60)
    
    # Run the bot
    sys.exit(run_bot())