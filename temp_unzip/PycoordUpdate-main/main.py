"""
Tower of Temptation PvP Statistics Discord Bot
Main entry point for Replit run button - runs the Discord bot directly
as required by rule #7 in rules.md (Stack Integrity Is Mandatory)
"""
import os
import sys
import logging
import asyncio
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

# Create a flag file to indicate we're running in a workflow
with open(".running_in_workflow", "w") as f:
    f.write(f"Started at {datetime.now()}")

if __name__ == "__main__":
    # Print a banner to make it clear the bot is starting
    print("=" * 60)
    print("  Tower of Temptation PvP Statistics Discord Bot")
    print("=" * 60)
    print(f"  Starting bot at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("  Press Ctrl+C to stop the bot")
    print("=" * 60)
    
    logger.info("Starting Tower of Temptation PvP Statistics Discord Bot")
    try:
        # Import and run the bot
        from bot import main as bot_main
        sys.exit(bot_main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Error starting bot: {e}", exc_info=True)
        sys.exit(1)