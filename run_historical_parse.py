"""
Enhanced script to manually trigger a historical parse for testing purposes

This script:
1. Directly calls the historical_parse method on the CSV processor
2. Uses extended date range to process more historical files
3. Includes detailed logging and error handling
4. Verifies database records after processing
"""
import asyncio
import json
import logging
import sys
import os
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Use DEBUG level to get more detailed logs
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("historical_parse.log")  # Also save logs to file
    ]
)
logger = logging.getLogger(__name__)

# Server ID to process
TARGET_SERVER_ID = "2eb00d14-8b8c-4371-8f75-facfe10f86cb"

# Set up test CSV files for local testing fallback
TEST_FILES = [
    "attached_assets/2025.03.27-00.00.00.csv",
    "attached_assets/2025.05.01-00.00.00.csv",
    "attached_assets/2025.05.03-00.00.00.csv"
]

async def main():
    """Run a historical parse with a small window for testing"""
    try:
        # Create a test bot for this task
        from bot import initialize_bot
        
        # Initialize the bot using the existing function
        logger.info("Initializing bot...")
        test_bot = await initialize_bot(force_sync=False)
        
        # Verify database connection
        logger.info("Verifying database connection...")
        
        # Get the already loaded CSV processor cog
        logger.info("Getting CSV processor cog...")
        from cogs.csv_processor import CSVProcessorCog
        
        # Get the already loaded cog instance
        csv_processor = test_bot.get_cog("CSVProcessorCog")
        if not csv_processor:
            logger.error("CSV processor cog not found!")
            return
        
        logger.info(f"Running historical parse for server {TARGET_SERVER_ID} with 7-day window")
        
        # First verify that server configuration is available
        configs = await csv_processor._get_server_configs()
        if TARGET_SERVER_ID not in configs:
            logger.error(f"Server configuration for {TARGET_SERVER_ID} not found!")
            return
        
        logger.info(f"Found server configuration: {configs[TARGET_SERVER_ID]['hostname']}:{configs[TARGET_SERVER_ID]['port']}")
        
        # Run historical parse with a 7-day window for more thorough testing
        files_processed, events_processed = await csv_processor.run_historical_parse(
            TARGET_SERVER_ID, days=7
        )
        
        # Check database for processed events
        kills = await test_bot.db.kills.count_documents({
            "server_id": TARGET_SERVER_ID,
            "timestamp": {"$gte": datetime.now() - timedelta(days=7)}
        })
        
        suicides = await test_bot.db.kills.count_documents({
            "server_id": TARGET_SERVER_ID,
            "timestamp": {"$gte": datetime.now() - timedelta(days=7)},
            "is_suicide": True
        })
        
        logger.info(f"Historical parse complete: {files_processed} files processed, {events_processed} events processed")
        logger.info(f"Database records: {kills} kills total, including {suicides} suicides")
        
    except Exception as e:
        logger.error(f"Error running historical parse: {e}", exc_info=True)
        
if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())