"""
List all guilds and channels the bot has access to
"""

import asyncio
import logging
import sys
import discord

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

async def main():
    """List all guilds and channels the bot has access to"""
    # Import bot
    sys.path.append('.')
    from bot import initialize_bot
    
    # Initialize bot
    bot = await initialize_bot(force_sync=False)
    if not bot:
        logger.error("Failed to initialize bot")
        return
    
    try:
        logger.info(f"Bot initialized as {bot.user} (ID: {bot.user.id})")
        
        # Print guilds
        logger.info(f"Bot is in {len(bot.guilds)} guilds:")
        for guild in bot.guilds:
            logger.info(f"Guild: {guild.name} (ID: {guild.id})")
            
            # List channels
            text_channels = guild.text_channels
            if text_channels:
                logger.info("  Text Channels:")
                for channel in text_channels:
                    logger.info(f"  - #{channel.name} (ID: {channel.id})")
            
            # List voice channels
            voice_channels = guild.voice_channels
            if voice_channels:
                logger.info("  Voice Channels:")
                for channel in voice_channels:
                    logger.info(f"  - 🔊 {channel.name} (ID: {channel.id})")
            
            # List categories
            categories = guild.categories
            if categories:
                logger.info("  Categories:")
                for category in categories:
                    logger.info(f"  - 📁 {category.name} (ID: {category.id})")
                    
        # Print all channels accessible via get_channel
        logger.info("\nChannels accessible via get_channel:")
        for guild in bot.guilds:
            for channel in guild.channels:
                test_channel = bot.get_channel(channel.id)
                status = "✅" if test_channel else "❌"
                logger.info(f"{status} Channel {channel.name} (ID: {channel.id})")
        
        # Try your specific channel
        target_id = 1360632422957449237
        channel = bot.get_channel(target_id)
        status = "✅" if channel else "❌"
        logger.info(f"\n{status} Target channel ID {target_id}: {'Found' if channel else 'Not found'}")
        
        # Try alternate methods to find the channel
        for guild in bot.guilds:
            found = discord.utils.get(guild.channels, id=target_id)
            if found:
                logger.info(f"Found channel with utils.get: {found.name} in {guild.name}")
        
    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        # Close bot
        await bot.close()

if __name__ == "__main__":
    asyncio.run(main())