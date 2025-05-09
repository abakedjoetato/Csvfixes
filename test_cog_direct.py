"""
Direct Test of CSV Processor Cog with Live SFTP Connection

This script directly tests the CSV processor cog with real SFTP connections
to verify our timestamp parsing fix in the production environment.
"""

import asyncio
import logging
import sys
from datetime import datetime, timedelta
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("cog_direct_test.log")
    ]
)

logger = logging.getLogger(__name__)

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
}

async def test_csv_processor_cog():
    """Test the CSV processor cog with live SFTP data"""
    logger.info("Starting direct test of CSV processor cog with real SFTP data")
    
    try:
        # Initialize database and bot
        sys.path.append('.')
        from bot import initialize_bot
        
        # Initialize the bot (but don't start it)
        bot = await initialize_bot(force_sync=False)
        if not bot:
            logger.error("Failed to initialize bot")
            return False
        
        # Get the CSV processor cog
        csv_processor = bot.get_cog("CSVProcessorCog")
        if not csv_processor:
            logger.error("CSV processor cog not found")
            return False
        
        logger.info("Successfully loaded CSV processor cog")
        
        # Set a cutoff date 60 days ago for testing purposes
        cutoff_date = datetime.now() - timedelta(days=60)
        
        # If the cog has a last_processed dictionary, set the entry for our server
        # to ensure it will process files going back 60 days
        if hasattr(csv_processor, 'last_processed'):
            csv_processor.last_processed[SERVER_ID] = cutoff_date
            logger.info(f"Set last_processed for server {SERVER_ID} to {cutoff_date}")
        
        # Process the server
        logger.info(f"Processing server {SERVER_ID}")
        
        try:
            # If the cog has a _process_server_csv_files method, use it
            if hasattr(csv_processor, '_process_server_csv_files'):
                files_processed, events_processed = await csv_processor._process_server_csv_files(SERVER_ID, SERVER_CONFIG)
                logger.info(f"Successfully processed {files_processed} files with {events_processed} events")
                
                # Success criteria
                if files_processed > 0:
                    logger.info("Timestamp parsing fix VERIFIED with real SFTP data!")
                    return True
                else:
                    logger.warning("No files were processed")
            else:
                logger.error("CSV processor cog doesn't have _process_server_csv_files method")
                return False
        except Exception as e:
            logger.error(f"Error processing server: {str(e)}")
            return False
        
        # If we got here without returning, it's a failure
        return False
        
    except Exception as e:
        logger.error(f"Unhandled error: {str(e)}")
        return False

async def main():
    """Main function"""
    try:
        success = await test_csv_processor_cog()
        
        if success:
            logger.info("CSV processor cog test PASSED with real SFTP data")
        else:
            logger.error("CSV processor cog test FAILED with real SFTP data")
            
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
    finally:
        logger.info("Test complete")

if __name__ == "__main__":
    asyncio.run(main())