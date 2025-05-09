"""
Script to manually trigger a historical parse for testing purposes

This will directly call the historical_parse method on the CSV processor
with a small window to make sure our enhanced event processing is working.
"""
import asyncio
import logging
import sys
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Server ID to process
TARGET_SERVER_ID = "2eb00d14-8b8c-4371-8f75-facfe10f86cb"

async def main():
    """Run a historical parse with a small window for testing"""
    try:
        # Create a test bot for this task
        from bot import PvPBot
        
        # Create a bot instance just for this test
        test_bot = PvPBot()
        
        # Initialize database connection
        logger.info("Initializing database connection...")
        await test_bot.init_db()
        
        # Load the CSV processor cog
        logger.info("Loading CSV processor cog...")
        from cogs.csv_processor import CSVProcessorCog
        csv_processor = CSVProcessorCog(test_bot)
        await test_bot.add_cog(csv_processor)
        
        logger.info(f"Running historical parse for server {TARGET_SERVER_ID} with 2-day window")
        
        # Run historical parse with a 2-day window
        files_processed, events_processed = await csv_processor.run_historical_parse(
            TARGET_SERVER_ID, days=2
        )
        
        logger.info(f"Historical parse complete: {files_processed} files processed, {events_processed} events processed")
        
    except Exception as e:
        logger.error(f"Error running historical parse: {e}", exc_info=True)
        
if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())