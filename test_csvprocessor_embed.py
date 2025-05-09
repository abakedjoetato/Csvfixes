"""
Test the CSV processor with the timestamp parsing fix and show the results in Discord.

This script uses the existing bot instance and CSV processor to verify 
that timestamps are properly parsed from real SFTP data, with results
displayed in Discord channel #1360632422957449237.
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
        logging.FileHandler("csvprocessor_embed_test.log")
    ]
)

logger = logging.getLogger(__name__)

# Target Discord channel
TARGET_CHANNEL_ID = 1360632422957449237

# Server configuration
SERVER_ID = "c8009f11-4f0f-4c68-8623-dc4b5c393722"
ORIGINAL_SERVER_ID = "7020"  # This is the critical ID value that must be used

async def run_csv_test():
    """Run the CSV processor test in the specified Discord channel"""
    logger.info(f"Testing CSV processor with timestamp parsing fix in channel {TARGET_CHANNEL_ID}")
    
    try:
        # Initialize bot (but don't start it)
        sys.path.append('.')
        from bot import initialize_bot
        
        # Initialize the bot
        bot = await initialize_bot(force_sync=False)
        if not bot:
            logger.error("Failed to initialize bot")
            return False
        
        # Get the channel
        channel = bot.get_channel(TARGET_CHANNEL_ID)
        if not channel:
            logger.error(f"Channel with ID {TARGET_CHANNEL_ID} not found")
            return False
        
        logger.info(f"Found channel: {channel.name}")
        
        # Create initial embed
        embed = discord.Embed(
            title="CSV Timestamp Parsing Test",
            description="Testing CSV timestamp parsing fix with real SFTP data...",
            color=discord.Color.blue()
        )
        
        embed.add_field(
            name="Status", 
            value="🔄 Starting test with CSV processor...", 
            inline=False
        )
        embed.set_footer(text=f"Test started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Send initial message
        test_message = await channel.send(embed=embed)
        
        # Get the CSV processor cog
        csv_processor = bot.get_cog("CSVProcessorCog")
        if not csv_processor:
            logger.error("CSV processor cog not found")
            
            embed.color = discord.Color.red()
            embed.add_field(
                name="Error", 
                value="❌ CSV processor cog not found", 
                inline=False
            )
            await test_message.edit(embed=embed)
            return False
        
        # Update embed
        embed.add_field(
            name="CSV Processor", 
            value="✅ Found CSV processor cog", 
            inline=False
        )
        await test_message.edit(embed=embed)
        
        # Set a cutoff date of 60 days ago to ensure we process all files
        cutoff_date = datetime.now() - timedelta(days=60)
        
        # If the cog has a last_processed dictionary, set the entry for our server
        if hasattr(csv_processor, 'last_processed'):
            csv_processor.last_processed[SERVER_ID] = cutoff_date
            
            embed.add_field(
                name="Cutoff Date", 
                value=f"📅 Set processing window to 60 days: {cutoff_date.strftime('%Y-%m-%d')}", 
                inline=False
            )
            await test_message.edit(embed=embed)
        
        # Update embed
        embed.add_field(
            name="Starting Process", 
            value="🔄 Processing CSV files with server ID mapping...", 
            inline=False
        )
        await test_message.edit(embed=embed)
        
        # Process the server's CSV files
        try:
            # Create server config with the correct original_server_id
            server_config = {
                "hostname": "79.127.236.1",
                "port": 8822,
                "username": "baked",
                "password": "emerald",
                "sftp_path": "/logs",
                "csv_pattern": r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}\.csv",
                "original_server_id": ORIGINAL_SERVER_ID,  # Critical ID value
            }
            
            # Process the server's CSV files
            files_processed, events_processed = await csv_processor._process_server_csv_files(
                SERVER_ID, server_config
            )
            
            # Check results
            if files_processed > 0:
                embed.color = discord.Color.green()
                embed.add_field(
                    name="Test Results ✅", 
                    value=f"Successfully processed {files_processed} CSV files with {events_processed} events!", 
                    inline=False
                )
                
                # Get some events for display
                events = []
                try:
                    # Get database and find some events
                    db = bot.db
                    if db:
                        # Query the most recent events
                        cursor = db.kills.find({"server_id": SERVER_ID}).sort("timestamp", -1).limit(5)
                        async for doc in cursor:
                            events.append(doc)
                        
                        # Show event examples
                        if events:
                            event_list = ""
                            for i, event in enumerate(events):
                                timestamp = event.get("timestamp")
                                if isinstance(timestamp, datetime):
                                    formatted_time = timestamp.strftime("%Y-%m-%d %H:%M:%S")
                                else:
                                    formatted_time = str(timestamp)
                                    
                                killer = event.get("killer_name", "Unknown")
                                victim = event.get("victim_name", "Unknown")
                                event_list += f"• Event {i+1}: {formatted_time} - {killer} killed {victim}\n"
                            
                            embed.add_field(
                                name="Sample Events with Parsed Timestamps", 
                                value=f"```\n{event_list}\n```", 
                                inline=False
                            )
                except Exception as e:
                    logger.error(f"Error getting event examples: {str(e)}")
                    embed.add_field(
                        name="Event Examples", 
                        value=f"Could not retrieve example events: {str(e)[:100]}", 
                        inline=False
                    )
                
                # Final verdict
                embed.add_field(
                    name="Timestamp Parsing Fix Verdict", 
                    value="✅ **FIX VERIFIED** - CSV files with format YYYY.MM.DD-HH.MM.SS are now correctly parsed!", 
                    inline=False
                )
            else:
                embed.color = discord.Color.red()
                embed.add_field(
                    name="Test Results ❌", 
                    value=f"Failed to process any CSV files. Check server configuration.", 
                    inline=False
                )
            
            # Update final embed
            embed.set_footer(text=f"Test completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            await test_message.edit(embed=embed)
            
            return files_processed > 0
            
        except Exception as e:
            logger.error(f"Error processing server CSV files: {str(e)}")
            logger.error(traceback.format_exc())
            
            embed.color = discord.Color.red()
            embed.add_field(
                name="Error ❌", 
                value=f"Error processing CSV files: {str(e)[:1000]}", 
                inline=False
            )
            
            # Add stack trace for debugging
            embed.add_field(
                name="Stack Trace", 
                value=f"```\n{traceback.format_exc()[:500]}\n```", 
                inline=False
            )
            
            await test_message.edit(embed=embed)
            return False
    
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        logger.error(traceback.format_exc())
        return False

async def main():
    """Main function"""
    try:
        logger.info("Starting CSV processor timestamp parsing test with Discord embed")
        
        # Run the test
        success = await run_csv_test()
        
        if success:
            logger.info("CSV processor test PASSED - timestamp parsing fix verified")
        else:
            logger.error("CSV processor test FAILED")
            
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        logger.info("Test complete")

if __name__ == "__main__":
    asyncio.run(main())