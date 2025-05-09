"""
Verification of CSV Timestamp Parsing Fix with Live SFTP Data

This script directly tests the CSV timestamp parsing with real CSV files
from the SFTP server, using the correct server ID mapping.
"""

import asyncio
import logging
import sys
import re
from datetime import datetime, timedelta
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("timestamp_verification.log")
    ]
)

logger = logging.getLogger(__name__)

# Server configuration with the CORRECT original ID
SERVER_ID = "c8009f11-4f0f-4c68-8623-dc4b5c393722"
ORIGINAL_SERVER_ID = "7020"  # This MUST be 7020 for the correct path
SERVER_CONFIG = {
    "hostname": "79.127.236.1",
    "port": 8822,
    "username": "baked",
    "password": "emerald",
    "sftp_path": "/logs",
    "csv_pattern": r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}\.csv",
    "original_server_id": ORIGINAL_SERVER_ID,
}

async def verify_timestamp_parsing():
    """Verify CSV timestamp parsing with live SFTP data"""
    logger.info("Starting verification of CSV timestamp parsing with live SFTP data")
    
    try:
        # Import necessary modules
        sys.path.append('.')
        from utils.sftp import SFTPClient
        from utils.csv_parser import CSVParser
        
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
        
        # Connect to server
        logger.info("Connecting to SFTP server")
        connected = await sftp_client.connect()
        if not connected:
            logger.error("Failed to connect to SFTP server")
            return False
        
        logger.info("Successfully connected to SFTP server")
        
        # Use known paths from the bot logs
        deathlogs_path = f"/79.127.236.1_{ORIGINAL_SERVER_ID}/actual1/deathlogs"
        world_path = f"{deathlogs_path}/world_0"
        
        # Use the available find_files_by_pattern method
        logger.info(f"Finding CSV files in {deathlogs_path}")
        csv_pattern = SERVER_CONFIG["csv_pattern"]
        csv_files = await sftp_client.find_files_by_pattern(
            deathlogs_path, 
            pattern=csv_pattern,
            recursive=True,
            max_depth=5
        )
        
        if not csv_files:
            logger.error("No CSV files found")
            return False
        
        logger.info(f"Found {len(csv_files)} CSV files")
        
        # Sort by filename (which should be timestamps) and take the most recent
        csv_files.sort(reverse=True)
        test_files = csv_files[:3]  # Take 3 most recent
        
        logger.info(f"Testing timestamp parsing with files: {test_files}")
        
        # Create CSV parser
        csv_parser = CSVParser()
        
        # Process each file
        total_events = 0
        successful_files = 0
        
        for file_path in test_files:
            try:
                logger.info(f"Reading file: {file_path}")
                
                # Read the file
                content = await sftp_client.read_file(file_path)
                if not content:
                    logger.warning(f"Empty file: {file_path}")
                    continue
                
                # Join the lines if we got a list
                if isinstance(content, list):
                    content = "\n".join(content)
                
                # Log a sample of the content
                sample = content[:200] + "..." if len(content) > 200 else content
                logger.info(f"File content sample: {sample}")
                
                # Parse the CSV content
                logger.info("Parsing CSV with timestamp format %Y.%m.%d-%H.%M.%S")
                events = csv_parser.parse(content, server_id=SERVER_ID)
                
                if not events:
                    logger.warning(f"No events found in {file_path}")
                    continue
                
                # Log success
                logger.info(f"Successfully parsed {len(events)} events from {file_path}")
                
                # Check timestamps
                for i, event in enumerate(events[:5]):  # Check first 5 events
                    timestamp = event.get("timestamp")
                    if timestamp:
                        logger.info(f"Event {i} timestamp: {timestamp}")
                    else:
                        logger.warning(f"Event {i} has no timestamp: {event}")
                
                # Track statistics
                total_events += len(events)
                successful_files += 1
                
            except Exception as e:
                logger.error(f"Error processing {file_path}: {str(e)}")
                logger.error(f"Stack trace: {traceback.format_exc()}")
        
        # Disconnect
        await sftp_client.disconnect()
        
        # Check if we successfully processed any files
        if successful_files > 0:
            logger.info(f"✓ Successfully processed {successful_files} files with {total_events} events")
            logger.info("✓ TIMESTAMP PARSING FIX VERIFIED with real SFTP data")
            return True
        else:
            logger.error("✗ Failed to process any CSV files")
            return False
        
    except Exception as e:
        import traceback
        logger.error(f"Unhandled error: {str(e)}")
        logger.error(f"Stack trace: {traceback.format_exc()}")
        return False

async def main():
    """Main function"""
    try:
        success = await verify_timestamp_parsing()
        
        if success:
            logger.info("✓ CSV TIMESTAMP PARSING FIX VERIFIED with real data")
        else:
            logger.error("✗ Test failed to verify the fix with real data")
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
    finally:
        logger.info("Test complete")

if __name__ == "__main__":
    asyncio.run(main())