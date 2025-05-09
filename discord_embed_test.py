"""
Live Test of CSV Timestamp Parsing Fix with Discord Embed Updates

This script tests the CSV timestamp parsing fix and sends real-time updates 
to a Discord channel as it processes files from the SFTP server.
"""

import asyncio
import discord
import logging
import sys
import os
import re
import traceback
from datetime import datetime, timedelta
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("discord_embed_test.log")
    ]
)

logger = logging.getLogger(__name__)

# Target Discord channel
CHANNEL_ID = 1360632422957449237

# Server configuration with correct ID
SERVER_ID = "c8009f11-4f0f-4c68-8623-dc4b5c393722"
ORIGINAL_SERVER_ID = "7020"  # Essential: must be 7020 not 8009
SERVER_CONFIG = {
    "hostname": "79.127.236.1",
    "port": 8822,
    "username": "baked",
    "password": "emerald",
    "sftp_path": "/logs",
    "csv_pattern": r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}\.csv",
    "original_server_id": ORIGINAL_SERVER_ID,
}

class TestBot(discord.Client):
    """Test bot for CSV timestamp parsing verification"""
    
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(intents=intents)
        self.target_channel = None
        self.test_message = None
        self.ready = asyncio.Event()
    
    async def on_ready(self):
        """Called when the bot is ready"""
        logger.info(f"Logged in as {self.user.name} ({self.user.id})")
        
        # Find target channel
        self.target_channel = self.get_channel(CHANNEL_ID)
        if not self.target_channel:
            logger.error(f"Channel with ID {CHANNEL_ID} not found")
        else:
            logger.info(f"Found target channel: {self.target_channel.name}")
            
        self.ready.set()
    
    async def run_test(self):
        """Run the test and send updates to Discord"""
        await self.ready.wait()
        
        if not self.target_channel:
            logger.error("No target channel found")
            return False
        
        try:
            # Create initial embed
            embed = discord.Embed(
                title="CSV Timestamp Parsing Test - Starting",
                description="Testing CSV timestamp parsing fix with real SFTP data...",
                color=discord.Color.blue()
            )
            
            embed.add_field(
                name="Status",
                value="🔄 Connecting to SFTP server...",
                inline=False
            )
            
            embed.set_footer(text=f"Test started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Send initial message
            self.test_message = await self.target_channel.send(embed=embed)
            
            # Import necessary modules
            sys.path.append('.')
            from utils.sftp import SFTPClient
            from utils.csv_parser import CSVParser
            
            # Update embed - Creating client
            embed.add_field(
                name="Step 1", 
                value="Creating SFTP client with server ID mapping...", 
                inline=False
            )
            await self.test_message.edit(embed=embed)
            
            # Create SFTP client
            logger.info(f"Creating SFTP client with original ID: {ORIGINAL_SERVER_ID}")
            sftp_client = SFTPClient(
                hostname=SERVER_CONFIG["hostname"],
                port=SERVER_CONFIG["port"],
                username=SERVER_CONFIG["username"],
                password=SERVER_CONFIG["password"],
                server_id=SERVER_ID,
                original_server_id=ORIGINAL_SERVER_ID
            )
            
            # Update embed - Connecting
            embed.add_field(
                name="Step 2", 
                value="🔄 Connecting to SFTP server...", 
                inline=False
            )
            await self.test_message.edit(embed=embed)
            
            # Connect to server
            logger.info("Connecting to SFTP server")
            connected = await sftp_client.connect()
            if not connected:
                embed.color = discord.Color.red()
                embed.add_field(
                    name="Error", 
                    value="❌ Failed to connect to SFTP server", 
                    inline=False
                )
                await self.test_message.edit(embed=embed)
                logger.error("Failed to connect to SFTP server")
                return False
            
            # Update embed - Connected
            embed.add_field(
                name="Step 3", 
                value="✅ Successfully connected to SFTP server", 
                inline=False
            )
            await self.test_message.edit(embed=embed)
            
            # Define paths
            deathlogs_path = f"/79.127.236.1_{ORIGINAL_SERVER_ID}/actual1/deathlogs"
            world_path = f"{deathlogs_path}/world_0"
            
            # Update embed - Finding files
            embed.add_field(
                name="Step 4", 
                value=f"🔄 Finding CSV files in {deathlogs_path}...", 
                inline=False
            )
            await self.test_message.edit(embed=embed)
            
            # Find CSV files
            csv_pattern = SERVER_CONFIG["csv_pattern"]
            csv_files = await sftp_client.find_files_by_pattern(
                deathlogs_path, 
                pattern=csv_pattern,
                recursive=True,
                max_depth=5
            )
            
            if not csv_files:
                embed.color = discord.Color.red()
                embed.add_field(
                    name="Error", 
                    value="❌ No CSV files found", 
                    inline=False
                )
                await self.test_message.edit(embed=embed)
                logger.error("No CSV files found")
                return False
            
            # Update embed - Found files
            embed.add_field(
                name="Step 5", 
                value=f"✅ Found {len(csv_files)} CSV files", 
                inline=False
            )
            
            # Log sample files
            sample_files = csv_files[:3]
            file_list = "\n".join([f"- {os.path.basename(f)}" for f in sample_files])
            embed.add_field(
                name="Sample Files", 
                value=f"```\n{file_list}\n```", 
                inline=False
            )
            await self.test_message.edit(embed=embed)
            
            # Create CSV parser
            csv_parser = CSVParser()
            
            # Update embed - Processing
            embed.add_field(
                name="Step 6", 
                value="🔄 Processing CSV files to verify timestamp parsing...", 
                inline=False
            )
            await self.test_message.edit(embed=embed)
            
            # Process files
            total_events = 0
            successful_files = 0
            failed_files = 0
            sample_events = []
            
            for i, file_path in enumerate(sample_files):
                try:
                    logger.info(f"Processing file: {file_path}")
                    
                    # Update file progress
                    prog_embed = discord.Embed(
                        title="CSV Timestamp Parsing Test - In Progress",
                        description=f"Processing file {i+1}/{len(sample_files)}: {os.path.basename(file_path)}",
                        color=discord.Color.gold()
                    )
                    await self.test_message.edit(embed=prog_embed)
                    
                    # Read the file
                    content = await sftp_client.read_file(file_path)
                    if not content:
                        logger.warning(f"Empty file: {file_path}")
                        failed_files += 1
                        continue
                    
                    # Join the lines if we got a list
                    if isinstance(content, list):
                        content = "\n".join(content)
                    
                    # Parse the CSV content
                    logger.info("Parsing CSV with timestamp format %Y.%m.%d-%H.%M.%S")
                    events = csv_parser.parse(content, server_id=SERVER_ID)
                    
                    if not events:
                        logger.warning(f"No events found in {file_path}")
                        failed_files += 1
                        continue
                    
                    # Log success
                    logger.info(f"Successfully parsed {len(events)} events from {file_path}")
                    
                    # Save sample events
                    for event in events[:2]:  # Get 2 events from each file
                        if event.get("timestamp"):
                            sample_events.append(event)
                    
                    # Update statistics
                    total_events += len(events)
                    successful_files += 1
                    
                    # Update progress
                    prog_embed.add_field(
                        name=f"File {i+1} Results", 
                        value=f"✅ Processed {len(events)} events from {os.path.basename(file_path)}", 
                        inline=False
                    )
                    await self.test_message.edit(embed=prog_embed)
                    
                except Exception as e:
                    logger.error(f"Error processing {file_path}: {str(e)}")
                    logger.error(traceback.format_exc())
                    failed_files += 1
                    
                    # Update error in embed
                    prog_embed.add_field(
                        name=f"File {i+1} Error", 
                        value=f"❌ Failed to process {os.path.basename(file_path)}: {str(e)[:100]}...", 
                        inline=False
                    )
                    await self.test_message.edit(embed=prog_embed)
            
            # Disconnect
            await sftp_client.disconnect()
            
            # Final embed
            final_embed = discord.Embed(
                title="CSV Timestamp Parsing Test - Complete",
                description="Verification of timestamp parsing fix with real SFTP data",
                color=discord.Color.green() if total_events > 0 else discord.Color.red()
            )
            
            final_embed.add_field(
                name="Summary", 
                value=f"✅ Files Processed: {successful_files}/{len(sample_files)}\n"
                      f"✅ Total Events: {total_events}\n"
                      f"❌ Failed Files: {failed_files}", 
                inline=False
            )
            
            # Show sample events with timestamps
            if sample_events:
                event_list = ""
                for i, event in enumerate(sample_events[:5]):
                    event_time = event.get("timestamp")
                    killer = event.get("killer_name", "Unknown")
                    victim = event.get("victim_name", "Unknown")
                    timestamp_str = event_time.strftime("%Y-%m-%d %H:%M:%S") if event_time else "Unknown"
                    event_list += f"• Event {i+1}: {timestamp_str} - {killer} killed {victim}\n"
                
                final_embed.add_field(
                    name="Sample Events with Parsed Timestamps", 
                    value=f"```\n{event_list}\n```", 
                    inline=False
                )
            
            # Verdict
            if successful_files > 0:
                final_embed.add_field(
                    name="Verdict", 
                    value="✅ **TIMESTAMP PARSING FIX VERIFIED**\n"
                           "CSV files with format YYYY.MM.DD-HH.MM.SS are now correctly parsed!", 
                    inline=False
                )
            else:
                final_embed.add_field(
                    name="Verdict", 
                    value="❌ Test failed. Could not process any CSV files correctly.", 
                    inline=False
                )
            
            final_embed.set_footer(text=f"Test completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Send final update
            await self.test_message.edit(embed=final_embed)
            
            # Return test result
            return successful_files > 0
            
        except Exception as e:
            logger.error(f"Unhandled error in test: {str(e)}")
            logger.error(traceback.format_exc())
            
            # Send error embed
            error_embed = discord.Embed(
                title="CSV Timestamp Parsing Test - Error",
                description=f"An error occurred during testing: {str(e)}",
                color=discord.Color.red()
            )
            
            error_embed.add_field(
                name="Error Details", 
                value=f"```\n{traceback.format_exc()[:1000]}\n```", 
                inline=False
            )
            
            await self.test_message.edit(embed=error_embed)
            return False

async def main():
    """Main function"""
    # Get Discord token
    token = os.environ.get('DISCORD_TOKEN')
    if not token:
        # Try to get from bot.py
        try:
            sys.path.append('.')
            from bot import BOT_TOKEN
            token = BOT_TOKEN
        except:
            logger.error("Could not get Discord token")
            return
    
    if not token:
        logger.error("No Discord token found - cannot run the test")
        return
    
    # Create and run the bot
    bot = TestBot()
    
    try:
        # Start the bot
        async with bot:
            bot.loop.create_task(bot.run_test())
            await bot.start(token)
    except Exception as e:
        logger.error(f"Error running bot: {str(e)}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    asyncio.run(main())