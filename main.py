"""
Emeralds Killfeed PvP Statistics Discord Bot
Main entry point for Replit run button - runs the Discord bot directly
as required by rule #7 in rules.md (Stack Integrity Is Mandatory)
"""
import os
import sys
import logging
import asyncio
import traceback
import signal
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
logger = logging.getLogger('main')

# Set higher log level for some verbose libraries
logging.getLogger('discord.gateway').setLevel(logging.WARNING)
logging.getLogger('discord.client').setLevel(logging.WARNING)
logging.getLogger('discord.http').setLevel(logging.WARNING)

# Create a flag file to indicate we're running in a workflow
with open(".running_in_workflow", "w") as f:
    f.write(f"Started at {datetime.now()}")

# Track when we last restarted
def record_restart():
    try:
        with open("restart_log.txt", "a") as f:
            f.write(f"{datetime.now()}: Bot restarted\n")
    except Exception as e:
        logger.error(f"Failed to record restart: {e}")

# Handle signals gracefully
def signal_handler(signum, frame):
    sig_name = signal.Signals(signum).name
    logger.warning(f"Received signal {sig_name} ({signum})")
    if signum in (signal.SIGINT, signal.SIGTERM):
        logger.info("Bot stopping due to termination signal")
        sys.exit(0)

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

if __name__ == "__main__":
    # Record this restart
    record_restart()
    
    # Print a banner to make it clear the bot is starting
    print("=" * 60)
    print("  Emeralds Killfeed PvP Statistics Discord Bot")
    print("=" * 60)
    print(f"  Starting bot at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("  Press Ctrl+C to stop the bot")
    print("=" * 60)
    
    logger.info("Starting Emeralds Killfeed PvP Statistics Discord Bot")
    try:
        # Import and run the bot
        from bot import main as bot_main
        exit_code = bot_main()
        logger.info(f"Bot exited with code: {exit_code}")
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Error starting bot: {e}", exc_info=True)
        logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)