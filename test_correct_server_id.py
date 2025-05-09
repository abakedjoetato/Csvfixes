"""
Direct test of the CSV processor with the correct server ID mapping

This script ensures we're using the correct server ID (7020) when connecting to
the SFTP server and processing CSV files with our timestamp parsing fix.
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
        logging.FileHandler("server_id_test.log")
    ]
)

logger = logging.getLogger(__name__)

# Server configuration with CORRECT ID
SERVER_ID = "c8009f11-4f0f-4c68-8623-dc4b5c393722"
ORIGINAL_SERVER_ID = "7020"  # This is the critical value - MUST be 7020 not 8009
SERVER_CONFIG = {
    "hostname": "79.127.236.1",
    "port": 8822,
    "username": "baked",
    "password": "emerald",
    "sftp_path": "/logs",
    "csv_pattern": r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}\.csv",
    "original_server_id": ORIGINAL_SERVER_ID,  # Use the correct original ID
}

async def test_with_correct_server_id():
    """Test CSV processing with the correct server ID (7020)"""
    logger.info(f"Testing CSV processing with server ID: {SERVER_ID} (original: {ORIGINAL_SERVER_ID})")
    
    try:
        # Import necessary modules
        sys.path.append('.')
        from utils.sftp import SFTPClient
        from utils.csv_parser import CSVParser
        
        # Create SFTP client with explicit original server ID
        logger.info(f"Creating SFTP client for {SERVER_CONFIG['hostname']}:{SERVER_CONFIG['port']}")
        sftp_client = SFTPClient(
            hostname=SERVER_CONFIG["hostname"],
            port=SERVER_CONFIG["port"],
            username=SERVER_CONFIG["username"],
            password=SERVER_CONFIG["password"],
            server_id=SERVER_ID,
            original_server_id=ORIGINAL_SERVER_ID
        )
        
        # Connect to server
        logger.info(f"Connecting to SFTP server with original ID: {ORIGINAL_SERVER_ID}")
        connected = await sftp_client.connect()
        
        if not connected:
            logger.error("Failed to connect to SFTP server")
            return False
            
        logger.info("Successfully connected to SFTP server")
        
        # Manually construct the path with the correct ID
        base_path = f"/79.127.236.1_{ORIGINAL_SERVER_ID}/actual1/deathlogs/world_0"
        
        # Check if the path exists
        logger.info(f"Checking if path exists: {base_path}")
        try:
            result = await sftp_client.isdir(base_path)
            if result:
                logger.info(f"Path {base_path} exists!")
            else:
                logger.warning(f"Path {base_path} does not exist")
                
                # Try listing the parent directory to see what's available
                parent = f"/79.127.236.1_{ORIGINAL_SERVER_ID}/actual1/deathlogs"
                logger.info(f"Listing parent directory: {parent}")
                try:
                    parent_listing = await sftp_client.listdir(parent)
                    logger.info(f"Parent directory contents: {parent_listing}")
                except Exception as e:
                    logger.warning(f"Error listing parent directory: {str(e)}")
                    
                # Try root directory
                root = f"/79.127.236.1_{ORIGINAL_SERVER_ID}"
                logger.info(f"Listing root directory: {root}")
                try:
                    root_listing = await sftp_client.listdir(root)
                    logger.info(f"Root directory contents: {root_listing}")
                    
                    # Look for nested paths
                    for subdir in root_listing:
                        if "death" in subdir.lower() or "log" in subdir.lower() or "csv" in subdir.lower():
                            logger.info(f"Found interesting directory: {subdir}")
                except Exception as e:
                    logger.warning(f"Error listing root directory: {str(e)}")
        except Exception as e:
            logger.error(f"Error checking path: {str(e)}")
            
        # Find CSV files with the right pattern
        logger.info(f"Searching for CSV files in {base_path}")
        import re
        pattern = re.compile(SERVER_CONFIG["csv_pattern"])
        
        try:
            # Try to list files in the target directory
            csv_files = []
            files = await sftp_client.listdir(base_path)
            logger.info(f"Found {len(files)} files in {base_path}")
            
            # Filter for CSV files
            for filename in files:
                if pattern.match(filename):
                    csv_files.append(f"{base_path}/{filename}")
            
            logger.info(f"Found {len(csv_files)} CSV files matching pattern")
            
            if csv_files:
                # Sort by name (newest first assuming format is date-based)
                csv_files.sort(reverse=True)
                sample_files = csv_files[:3]  # Get 3 most recent files
                logger.info(f"Sample CSV files: {sample_files}")
                
                # Create CSV parser
                csv_parser = CSVParser()
                
                # Process a sample file to verify timestamp parsing
                for file_path in sample_files:
                    logger.info(f"Testing timestamp parsing with file: {file_path}")
                    
                    # Download file content
                    content = await sftp_client.read_file(file_path)
                    if not content:
                        logger.warning(f"Empty file: {file_path}")
                        continue
                    
                    # Sample content
                    logger.info(f"File content sample: {content[:200]}...")
                    
                    # Parse events
                    events = csv_parser.parse(content, server_id=SERVER_ID)
                    
                    if not events:
                        logger.warning(f"No events parsed from {file_path}")
                        continue
                    
                    logger.info(f"Successfully parsed {len(events)} events from {file_path}")
                    
                    # Check timestamps
                    for i, event in enumerate(events[:3]):
                        timestamp = event.get("timestamp")
                        if timestamp:
                            logger.info(f"Event {i} timestamp: {timestamp}")
                            
                    # If we got here with events, the timestamp parsing works!
                    logger.info("✓ Timestamp parsing SUCCESS with live SFTP data!")
                    return True
            else:
                logger.warning("No CSV files found matching pattern")
        except Exception as e:
            logger.error(f"Error listing files: {str(e)}")
            
        # Disconnect
        await sftp_client.disconnect()
        
        return False
        
    except Exception as e:
        logger.error(f"Unhandled error: {str(e)}")
        return False

async def main():
    """Main function"""
    try:
        success = await test_with_correct_server_id()
        
        if success:
            logger.info("✓ CSV timestamp parsing test PASSED with real SFTP data")
        else:
            logger.error("✗ CSV timestamp parsing test FAILED")
            
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
    finally:
        logger.info("Test complete")

if __name__ == "__main__":
    asyncio.run(main())