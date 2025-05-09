#!/usr/bin/env python3
"""
Tower of Temptation PvP Statistics Discord Bot
Run script for the Discord bot
"""
import os
import sys
import asyncio
import logging
from bot import main as bot_main

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bot.log')
    ]
)
logger = logging.getLogger('run_bot')

if __name__ == "__main__":
    logger.info("Starting Tower of Temptation PvP Statistics Discord Bot")
    try:
        # Run the bot
        sys.exit(bot_main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Error starting bot: {e}", exc_info=True)
        sys.exit(1)