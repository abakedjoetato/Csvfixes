"""
List all available Discord channels and their IDs
"""

import asyncio
import discord
import logging
import sys
from pprint import pprint

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    """List all available Discord channels"""
    logger.info("Initializing bot to list channels")
    
    # Initialize bot
    sys.path.append('.')
    from bot import initialize_bot
    
    # Initialize the bot with the desired token
    bot = await initialize_bot(force_sync=False)
    
    if not bot:
        logger.error("Failed to initialize bot")
        return
    
    logger.info(f"Bot initialized as {bot.user}")
    
    # List all guilds and channels
    logger.info("Connected to the following guilds:")
    for guild in bot.guilds:
        logger.info(f"Guild: {guild.name} (ID: {guild.id})")
        
        # List text channels
        text_channels = guild.text_channels
        if text_channels:
            logger.info("  Text Channels:")
            for channel in text_channels:
                logger.info(f"   - #{channel.name} (ID: {channel.id})")
        
        # List voice channels
        voice_channels = guild.voice_channels
        if voice_channels:
            logger.info("  Voice Channels:")
            for channel in voice_channels:
                logger.info(f"   - 🔊 {channel.name} (ID: {channel.id})")
        
        # List categories
        categories = guild.categories
        if categories:
            logger.info("  Categories:")
            for category in categories:
                logger.info(f"   - 📁 {category.name} (ID: {category.id})")
    
    # Close the bot
    await bot.close()

if __name__ == "__main__":
    asyncio.run(main())