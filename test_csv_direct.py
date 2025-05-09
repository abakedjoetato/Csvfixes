"""
Direct CSV Processing Test

This script directly tests the CSV processor without the Discord bot to verify
our timestamp parsing fix with real SFTP connections and CSV files.
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
        logging.FileHandler("csv_direct_test.log")
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

async def test_csv_timestamp_parsing():
    """Test CSV timestamp parsing with real SFTP connection"""
    logger.info("Starting direct test of CSV timestamp parsing with real SFTP data")
    
    try:
        # Import necessary modules
        sys.path.append('.')
        from utils.sftp import SFTPClient
        from utils.csv_parser import CSVParser
        
        # Create SFTP client
        logger.info(f"Creating SFTP client for {SERVER_CONFIG['hostname']}:{SERVER_CONFIG['port']}")
        sftp_client = SFTPClient(
            hostname=SERVER_CONFIG["hostname"],
            port=SERVER_CONFIG["port"],
            username=SERVER_CONFIG["username"],
            password=SERVER_CONFIG["password"],
            server_id=SERVER_ID,
            original_server_id=SERVER_CONFIG["original_server_id"]
        )
        
        # Connect to server
        logger.info("Connecting to SFTP server")
        connected = await sftp_client.connect()
        
        if not connected:
            logger.error("Failed to connect to SFTP server")
            return False
            
        logger.info("Successfully connected to SFTP server")
        
        # Create CSV parser
        csv_parser = CSVParser()
        
        # Find CSV files
        csv_files = []
        
        # Check standard paths
        deathlogs_paths = [
            f"/79.127.236.1_{SERVER_CONFIG['original_server_id']}/actual1/deathlogs",
            f"/79.127.236.1_{SERVER_CONFIG['original_server_id']}/actual1/deathlogs/world_0",
            f"/79.127.236.1_{SERVER_CONFIG['original_server_id']}/deathlogs"
        ]
        
        for path in deathlogs_paths:
            logger.info(f"Checking for CSV files in {path}")
            try:
                # List files in directory
                files = await sftp_client.listdir(path)
                if files:
                    # Filter for CSV files matching our pattern
                    import re
                    pattern = re.compile(SERVER_CONFIG["csv_pattern"])
                    matching_files = [f"{path}/{f}" for f in files if pattern.match(f)]
                    
                    if matching_files:
                        logger.info(f"Found {len(matching_files)} CSV files in {path}")
                        csv_files.extend(matching_files)
                        break
            except Exception as e:
                logger.warning(f"Error listing files in {path}: {str(e)}")
        
        if not csv_files:
            logger.error("No CSV files found in any path")
            return False
            
        # Sort files by name (which should be timestamps) and take most recent ones
        csv_files.sort(reverse=True)
        test_files = csv_files[:5]  # Take the 5 most recent files
        
        logger.info(f"Found {len(csv_files)} CSV files, testing with the {len(test_files)} most recent")
        logger.info(f"Test files: {test_files}")
        
        # Process the files
        total_files = 0
        total_events = 0
        successful_files = 0
        failed_files = 0
        timestamps_by_format = {}
        
        for file_path in test_files:
            try:
                logger.info(f"Processing file: {file_path}")
                
                # Read file
                content = await sftp_client.read_file(file_path)
                
                if not content or not content.strip():
                    logger.warning(f"Empty file: {file_path}")
                    failed_files += 1
                    continue
                
                # Parse events
                events = csv_parser.parse(content, server_id=SERVER_ID)
                
                if not events:
                    logger.warning(f"No events found in {file_path}")
                    failed_files += 1
                    continue
                
                # Check timestamps
                formats_used = {}
                for event in events[:5]:  # Log details for first 5 events
                    timestamp = event.get("timestamp")
                    if timestamp:
                        # Track format used
                        format_used = csv_parser.last_format_used
                        formats_used[format_used] = formats_used.get(format_used, 0) + 1
                        timestamps_by_format[format_used] = timestamps_by_format.get(format_used, 0) + 1
                        
                        # Log sample event
                        logger.info(f"Event timestamp: {timestamp} using format: {format_used}")
                        logger.info(f"Event details: {json.dumps(event, default=str)}")
                
                # Log file results
                logger.info(f"Successfully processed {len(events)} events from {file_path}")
                logger.info(f"Formats used: {formats_used}")
                
                total_events += len(events)
                successful_files += 1
                
            except Exception as e:
                logger.error(f"Error processing {file_path}: {str(e)}")
                failed_files += 1
                
            total_files += 1
        
        # Disconnect
        await sftp_client.disconnect()
        
        # Log summary
        logger.info("=== CSV TIMESTAMP PARSING TEST SUMMARY ===")
        logger.info(f"Total files tested: {total_files}")
        logger.info(f"Successfully processed: {successful_files} ({successful_files/total_files*100:.1f}%)")
        logger.info(f"Failed: {failed_files} ({failed_files/total_files*100:.1f}%)")
        logger.info(f"Total events processed: {total_events}")
        logger.info("Format usage:")
        for format_name, count in timestamps_by_format.items():
            logger.info(f"  {format_name}: {count} events ({count/total_events*100:.1f}%)")
        
        return successful_files > 0
        
    except Exception as e:
        logger.error(f"Unhandled error: {str(e)}")
        return False

async def main():
    """Main function"""
    try:
        success = await test_csv_timestamp_parsing()
        
        if success:
            logger.info("CSV timestamp parsing test PASSED with real SFTP data")
        else:
            logger.error("CSV timestamp parsing test FAILED with real SFTP data")
            
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
    finally:
        logger.info("Test complete")

if __name__ == "__main__":
    asyncio.run(main())