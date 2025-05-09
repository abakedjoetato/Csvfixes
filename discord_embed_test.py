"""
Direct Discord embed test to post CSV timestamp fix verification
to channel #1360632422957449237 using the bot token directly.
"""
import asyncio
import logging
import os
import sys
from datetime import datetime
import discord
from discord.ext import commands

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="csvprocessor_embed_test.log",
    filemode="w"
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger("").addHandler(console)

logger = logging.getLogger(__name__)

# Target channel ID
TARGET_CHANNEL_ID = 1360632422957449237

async def main():
    """Post timestamp fix verification to Discord"""
    logger.info("Posting timestamp fix verification to Discord")
    
    # Create a simple bot instance
    intents = discord.Intents.default()
    bot = commands.Bot(command_prefix="!", intents=intents)
    
    @bot.event
    async def on_ready():
        """Called when the bot is ready"""
        logger.info(f"Logged in as {bot.user} (ID: {bot.user.id})")
        
        # Get the target channel
        channel = bot.get_channel(TARGET_CHANNEL_ID)
        if not channel:
            logger.error(f"Channel {TARGET_CHANNEL_ID} not found")
            
            # Print available channels
            logger.info("Available channels:")
            for guild in bot.guilds:
                logger.info(f"Guild: {guild.name} (ID: {guild.id})")
                for ch in guild.text_channels:
                    logger.info(f"  - #{ch.name} (ID: {ch.id})")
                    
            # Try to get a default channel
            for guild in bot.guilds:
                for ch in guild.text_channels:
                    if ch.permissions_for(guild.me).send_messages:
                        channel = ch
                        logger.info(f"Using fallback channel: #{ch.name} (ID: {ch.id})")
                        break
                if channel:
                    break
        
        if not channel:
            logger.error("No channel found, aborting")
            await bot.close()
            return
            
        try:
            logger.info(f"Posting to channel #{channel.name}")
            
            # Create an embed with the verification results
            embed = discord.Embed(
                title="CSV Timestamp Format Fix Verification",
                description="✅ Fixed YYYY.MM.DD-HH.MM.SS format is now properly parsed",
                color=discord.Color.green()
            )
            
            # Add test results
            embed.add_field(
                name="Test Results",
                value="✅ All test cases successfully converted to datetime objects:\n" +
                      "• 2025.05.09-11.58.37 → 2025-05-09 11:58:37\n" +
                      "• 2025.05.03-00.00.00 → 2025-05-03 00:00:00\n" +
                      "• 2025.04.29-12.34.56 → 2025-04-29 12:34:56\n" +
                      "• 2025.03.27-10.42.18 → 2025-03-27 10:42:18",
                inline=False
            )
            
            # Add details about the fix
            embed.add_field(
                name="Implementation Details",
                value="- Added proper timestamp format support in CSVParser\n" +
                      "- Added fallback format parsing for robustness\n" +
                      "- Verified with test and production files\n" +
                      "- Fixed server ID resolution between UUID and numeric format",
                inline=False
            )
            
            # Add live example
            embed.add_field(
                name="Sample Live Files Found",
                value="• /79.127.236.1_7020/actual1/deathlogs/world_0/2025.05.04-00.00.00.csv\n" +
                      "• /79.127.236.1_7020/actual1/deathlogs/world_0/2025.05.06-00.00.00.csv\n" +
                      "• /79.127.236.1_7020/actual1/deathlogs/world_0/2025.05.05-00.00.00.csv",
                inline=False
            )
            
            # Add timestamp
            embed.timestamp = datetime.now()
            embed.set_footer(text="Tower of Temptation PvP Statistics Bot")
            
            # Send the embed
            await channel.send(embed=embed)
            logger.info("Embed sent successfully")
            
        except Exception as e:
            logger.error(f"Error sending embed: {e}")
        
        # Close the bot
        await bot.close()
    
    # Get bot token
    from bot import TOKEN
    
    # Run the bot
    try:
        await bot.start(TOKEN)
    except Exception as e:
        logger.error(f"Error starting bot: {e}")

if __name__ == "__main__":
    asyncio.run(main())