#!/usr/bin/env python3
"""
Script to directly test CSV processing without going through Discord commands.
This will use the same code path as the /process_csv command but can be run from the CLI.
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(stream=sys.stdout)
    ]
)
logger = logging.getLogger('test_csv_processing')

async def main():
    logger.info("Starting direct CSV processing test")
    
    # Import necessary functions/classes
    try:
        from bot import load_bot
        from utils.server_utils import safe_standardize_server_id
        logger.info("Successfully imported required modules")
    except ImportError as e:
        logger.error(f"Failed to import required modules: {e}")
        return

    # Load the bot but don't start it
    bot = await load_bot(start=False)
    logger.info("Loaded bot instance")

    # Get the CSV processor cog
    csv_processor = bot.get_cog('CSVProcessorCog')
    if not csv_processor:
        logger.error("CSV processor cog not found")
        return
    
    logger.info("Found CSV processor cog")
    
    # Test server ID - use the one from your environment
    server_id = "aac5327a-1c8d-4d24-a607-9dbe2e2ab1c5"  # Example UUID
    
    # Get server configs as the command would do
    server_configs = await csv_processor._get_server_configs()
    if not server_configs:
        logger.error("No server configurations found")
        return
    
    logger.info(f"Found {len(server_configs)} server configurations")
    
    # Check if our server ID is in the configs
    if server_id not in server_configs:
        logger.error(f"Server ID {server_id} not found in configurations")
        return
    
    logger.info(f"Server {server_id} found in configurations")
    
    # Configure processing window
    hours = 24
    start_date = datetime.now() - timedelta(hours=hours)
    csv_processor.last_processed[server_id] = start_date
    
    # Process CSV files directly
    async with csv_processor.processing_lock:
        csv_processor.is_processing = True
        try:
            logger.info(f"Starting CSV processing for server {server_id}")
            files_processed, events_processed = await csv_processor._process_server_csv_files(
                server_id, server_configs[server_id], start_date=start_date
            )
            logger.info(f"CSV processing complete: processed {files_processed} files with {events_processed} events")
        except Exception as e:
            logger.error(f"Error in CSV processing: {e}")
        finally:
            csv_processor.is_processing = False
    
    logger.info("Test completed")

if __name__ == "__main__":
    asyncio.run(main())