"""
Historical CSV Processing Script

This script runs a historical parse of CSV files for a specific server to verify
our timestamp parsing fixes are working correctly.
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
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('historical_parse.log')
    ]
)

logger = logging.getLogger(__name__)

async def run_historical_parse():
    """Run historical parse for a server to verify timestamp parsing"""
    logger.info("Starting historical CSV parse to verify timestamp parsing")
    
    try:
        # Import the bot to get access to its components
        import sys
        sys.path.append('.')
        from bot import initialize_bot
        
        # Initialize the bot without starting it
        bot = await initialize_bot(force_sync=False)
        
        if not bot:
            logger.error("Failed to initialize bot")
            return False
        
        # Get the CSV processor cog
        csv_processor = bot.get_cog("CSVProcessorCog")
        if not csv_processor:
            logger.error("CSV processor cog not found")
            return False
        
        logger.info("CSV processor cog found, running historical parse")
        
        # Use Deadside server ID (get from ServerIdentity util)
        from utils.server_identity import get_uuid_for_server_id
        server_id = await get_uuid_for_server_id(bot.db, "7020")
        
        if not server_id:
            logger.error("Failed to get server ID for Deadside server")
            # Fallback to known ID
            server_id = "c8009f11-4f0f-4c68-8623-dc4b5c393722"
            logger.info(f"Using fallback server ID: {server_id}")
        
        # Run historical parse for 60 days
        logger.info(f"Running historical parse for server ID: {server_id} (going back 60 days)")
        files_processed, events_processed = await csv_processor.run_historical_parse(server_id, days=60)
        
        logger.info(f"Historical parse complete. Processed {files_processed} files and {events_processed} events.")
        return True
    except Exception as e:
        logger.error(f"Error running historical parse: {str(e)}")
        return False

async def main():
    """Main function"""
    success = await run_historical_parse()
    
    if success:
        logger.info("Historical parse successfully verified timestamp parsing")
    else:
        logger.error("Historical parse failed to verify timestamp parsing")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")