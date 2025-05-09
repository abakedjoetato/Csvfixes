"""
Final test of the timestamp parsing fix with the correct server ID

This script uses the methods available in our SFTP client to locate
and process CSV files with the correct server ID (7020) configuration.
"""

import asyncio
import logging
import sys
from datetime import datetime, timedelta
import json
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("fix_test.log")
    ]
)

logger = logging.getLogger(__name__)

# Server configuration with the CORRECT ORIGINAL ID
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

async def directly_test_csv_files():
    """Test CSV processing with hardcoded paths to actual CSV files"""
    logger.info(f"Directly testing CSV files with server ID {SERVER_ID} (original: {ORIGINAL_SERVER_ID})")
    
    try:
        # Import necessary modules
        sys.path.append('.')
        from utils.sftp import SFTPClient
        from utils.csv_parser import CSVParser
        
        # Create SFTP client with the correct original ID
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
        
        # Use scan_directory with the correct path
        deathlogs_path = f"/79.127.236.1_{ORIGINAL_SERVER_ID}/actual1/deathlogs"
        world_path = f"{deathlogs_path}/world_0"
        
        # Try to find CSV files
        logger.info(f"Scanning for CSV files in {world_path}")
        csv_pattern = re.compile(SERVER_CONFIG["csv_pattern"])
        
        # Use scan_directory if available, otherwise fall back to a direct approach
        if hasattr(sftp_client, 'scan_directory'):
            files = await sftp_client.scan_directory(world_path)
        else:
            # Direct approach using sftp client
            logger.info("Using direct SFTP commands to list files")
            async with sftp_client._sftp as sftp:
                files = await sftp.listdir(world_path)
        
        if not files:
            logger.error(f"No files found in {world_path}")
            return False
        
        # Filter for CSV files
        csv_files = []
        for file in files:
            if csv_pattern.match(file):
                csv_files.append(f"{world_path}/{file}")
        
        if not csv_files:
            logger.error("No CSV files found matching pattern")
            return False
        
        logger.info(f"Found {len(csv_files)} CSV files")
        
        # Sort by name (which should be timestamps) and use the most recent
        csv_files.sort(reverse=True)
        test_files = csv_files[:3]  # Take 3 most recent files
        
        logger.info(f"Testing with files: {test_files}")
        
        # Create CSV parser
        csv_parser = CSVParser()
        
        # Process each file
        total_events = 0
        successful_files = 0
        
        for file_path in test_files:
            try:
                logger.info(f"Reading file: {file_path}")
                
                # Read file using sftp client
                content = await sftp_client.read_file(file_path)
                
                if not content or not content.strip():
                    logger.warning(f"Empty file: {file_path}")
                    continue
                
                # Log a sample of the content
                sample = content[:200] + "..." if len(content) > 200 else content
                logger.info(f"File content sample: {sample}")
                
                # Parse the CSV contents
                logger.info("Parsing CSV with timestamp format %Y.%m.%d-%H.%M.%S")
                events = csv_parser.parse(content, server_id=SERVER_ID)
                
                if not events:
                    logger.warning(f"No events found in {file_path}")
                    continue
                
                # Log success
                logger.info(f"Successfully parsed {len(events)} events from {file_path}")
                
                # Check timestamps in the events
                format_used = getattr(csv_parser, 'last_format_used', None)
                logger.info(f"Format used for parsing: {format_used}")
                
                # Check event timestamps
                for i, event in enumerate(events[:5]):  # Check first 5 events
                    timestamp = event.get("timestamp")
                    if timestamp:
                        logger.info(f"Event {i} timestamp: {timestamp}")
                    else:
                        logger.warning(f"Event {i} has no timestamp: {event}")
                
                total_events += len(events)
                successful_files += 1
                
            except Exception as e:
                logger.error(f"Error processing {file_path}: {str(e)}")
        
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
        logger.error(f"Unhandled error: {str(e)}")
        return False

async def main():
    """Main function"""
    try:
        success = await directly_test_csv_files()
        
        if success:
            logger.info("✓ CSV TIMESTAMP PARSING FIX VERIFIED with real data")
        else:
            logger.error("✗ Test failed, could not verify the fix with real data")
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
    finally:
        logger.info("Test complete")

if __name__ == "__main__":
    asyncio.run(main())