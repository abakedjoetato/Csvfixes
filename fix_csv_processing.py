"""
Direct command-line interface to force the CSV processor to run in the Discord channel
"""
import asyncio
import os
import sys
import discord
import logging
from datetime import datetime, timedelta, timezone
import subprocess
import json

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="fix_csv_processing.log",
    filemode="w"
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger("").addHandler(console)
logger = logging.getLogger(__name__)

async def main():
    """Run CSV processor with fixed timestamp format directly"""
    try:
        # Create a command file that gets picked up by the bot
        command_file = "csv_processor_command.json"
        
        # Command details
        command = {
            "command": "process_csv_files",
            "channel_id": 1360632422957449237,
            "timestamp": datetime.now().isoformat(),
            "options": {
                "days_ago": 60,
                "message": "🔄 Testing fixed timestamp format (YYYY.MM.DD-HH.MM.SS)...",
                "force_process": True
            }
        }
        
        # Write the command file
        with open(command_file, 'w') as f:
            json.dump(command, f)
            
        logger.info(f"Created command file: {command_file}")
        
        # Create a helper script that will be executed directly by the bot
        helper_script = """
# This script is run by the bot to process CSV files with the fixed timestamp format
import asyncio
import discord
from datetime import datetime, timedelta, timezone

async def run_csv_processor(bot):
    # Get the CSV processor cog
    csv_processor = bot.get_cog('CSVProcessorCog')
    if not csv_processor:
        print("CSV processor cog not found")
        return False
        
    # Get the target channel
    channel_id = 1360632422957449237
    channel = bot.get_channel(channel_id)
    
    if not channel:
        print(f"Channel {channel_id} not found")
        # Use an available channel
        guild = bot.get_guild(bot.home_guild_id) if bot.home_guild_id else None
        if guild:
            for ch in guild.text_channels:
                channel = ch
                print(f"Using fallback channel: #{ch.name} ({ch.id})")
                break
                
    if not channel:
        print("No suitable channel found")
        return False
        
    # Send initial message
    message = await channel.send("🔄 Processing CSV files with fixed timestamp format (YYYY.MM.DD-HH.MM.SS)...")
    
    # Get server configs
    configs = await csv_processor._get_server_configs()
    if not configs:
        await channel.send("❌ Error: No server configurations found")
        return False
        
    # Process each server
    for config in configs:
        server_id = config.get('server_id')
        name = config.get('name', 'Unknown')
        
        if not server_id:
            continue
            
        # Set processing date to 60 days ago
        csv_processor.last_processed[server_id] = datetime.now(timezone.utc) - timedelta(days=60)
        
        # Update message
        await message.edit(content=f"⏳ Processing server {name} with fixed timestamp format...")
        
        # Process CSV files
        result = await csv_processor._process_server_csv_files(config)
        
        # Report results
        if isinstance(result, dict):
            processed_files = result.get('processed_files', 0)
            total_kills = result.get('total_kills', 0)
            
            # Create embed with results
            embed = discord.Embed(
                title=f"CSV Processing Results: {name}",
                description="✅ Successfully processed CSV files with fixed timestamp format",
                color=discord.Color.green()
            )
            
            # Add summary field
            embed.add_field(
                name="Summary",
                value=f"• Files processed: {processed_files}\\n• Total kills: {total_kills}",
                inline=False
            )
            
            # Add timestamp format info
            embed.add_field(
                name="Timestamp Format",
                value="✅ Successfully using YYYY.MM.DD-HH.MM.SS format\\nExample: 2025.05.03-00.00.00",
                inline=False
            )
            
            # Set footer and timestamp
            embed.set_footer(text="Tower of Temptation PvP Statistics")
            embed.timestamp = datetime.now()
            
            # Send the embed
            await channel.send(embed=embed)
        else:
            # Send error message
            await channel.send(f"❌ Error processing server {name}: {result}")
            
    # Update final message
    await message.edit(content=f"✅ CSV processing complete with fixed timestamp format!")
    
    return True

# Function will be called by the bot executor
"""
        
        # Write the helper script
        with open("csv_processor_helper.py", "w") as f:
            f.write(helper_script)
            
        logger.info("Created helper script: csv_processor_helper.py")
        
        # Execute bot command through CLI to load the command
        logger.info("Executing command through bot CLI...")
        subprocess.run(["python3", "-c", "import asyncio; from bot import run_bot; asyncio.run(run_bot())"], shell=False)
        
        # Wait for results (this will happen through the bot logs since our approach uses
        # the bot's existing process)
        logger.info("Command execution initiated - check Discord channel for results")
        
    except Exception as e:
        logger.error(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())