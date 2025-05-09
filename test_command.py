"""
Direct CSV Processing Test with Live SFTP Data

This script bypasses the Discord bot and directly tests the CSV processor
with our live SFTP connection to verify that our timestamp parsing fixes work
in a real-world production environment.
"""

import asyncio
import logging
import sys
from datetime import datetime, timedelta
import json
import importlib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("csv_test.log")
    ]
)

logger = logging.getLogger(__name__)

# Target channel for communication
TARGET_CHANNEL_ID = 1360632422957449237

# Configuration for the Deadside server
SERVER_ID = "c8009f11-4f0f-4c68-8623-dc4b5c393722"
SERVER_CONFIG = {
    "hostname": "79.127.236.1",
    "port": 8822,
    "username": "baked",
    "password": "emerald",
    "sftp_path": "/logs",
    "csv_pattern": r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}\.csv",
    "original_server_id": "7020",
    "guild_id": "1219706687980568769"
}

async def test_csv_processing():
    """Test CSV processing in live channel"""
    logger.info("Starting CSV processing test in live channel")
    
    try:
        # Import modules
        sys.path.append('.')
        from utils.sftp import SFTPClient
        from utils.csv_parser import CSVParser
        from datetime import datetime, timedelta
        
        logger.info("Starting live testing of CSV processing in Discord channel")
        
        # Import bot and database modules
        from utils.discord_compat import discord
        from discord.ext import commands
        from bot import initialize_bot
        
        # Initialize bot (this will connect to database)
        bot = await initialize_bot()
        
        # Try to find the channel
        channel = bot.get_channel(TARGET_CHANNEL_ID)
        if not channel:
            logger.error(f"Channel with ID {TARGET_CHANNEL_ID} not found")
            logger.error("Live testing FAILED")
            return False
        
        logger.info(f"Found channel: {channel.name}")
        
        # Load CSV Processor Cog
        csv_processor = bot.get_cog("CSVProcessorCog")
        if not csv_processor:
            logger.error("CSV Processor cog not found")
            await channel.send("❌ CSV Processor cog not loaded - test failed")
            return False
        
        # Get server configs - try to use the cog method if available
        try:
            server_configs = await csv_processor._get_server_configs()
        except Exception as e:
            logger.error(f"Error getting server configs from cog: {str(e)}")
            # Use our manual config
            server_configs = {SERVER_ID: SERVER_CONFIG}
        
        if not server_configs:
            logger.error("No server configurations found")
            await channel.send("❌ No server configurations found - test failed")
            return False
        
        # Process CSV files for each server
        for server_id, config in server_configs.items():
            # Log server info (except password)
            safe_config = {k: v for k, v in config.items() if k not in ["password", "sftp_password"]}
            logger.info(f"Processing server {server_id}: {safe_config}")
            
            # Try to use the cog method
            try:
                files_processed, events_processed = await csv_processor._process_server_csv_files(server_id, config)
                logger.info(f"Successfully processed {files_processed} files with {events_processed} events")
                
                # Report to channel
                await channel.send(f"✅ Live test successful! Processed {files_processed} files with {events_processed} events from server {server_id}")
                
                if files_processed > 0:
                    return True
                
            except Exception as e:
                logger.error(f"Error processing server {server_id}: {str(e)}")
                await channel.send(f"❌ Error processing server {server_id}: {str(e)}")
        
        # No files processed
        logger.error("No CSV files were processed")
        await channel.send("❌ No CSV files were processed - test failed")
        return False
        
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        return False

async def main():
    """Main function for the test script"""
    try:
        logger.info("Starting live testing of CSV processing")
        
        success = await test_csv_processing()
        
        if success:
            logger.info("CSV processing test PASSED")
        else:
            logger.error("CSV processing test FAILED")
            
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
    finally:
        logger.info("Test complete")

if __name__ == "__main__":
    asyncio.run(main())