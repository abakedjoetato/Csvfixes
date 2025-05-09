"""
SFTP Direct Connection Test

This script directly tests the SFTP connection to verify that real CSV files
are being properly accessed and parsed with the correct timestamp format.
"""

import asyncio
import logging
import sys
import os
import json
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('sftp_test.log')
    ]
)

logger = logging.getLogger(__name__)

async def test_sftp_connection():
    """Test SFTP connection and CSV processing"""
    logger.info("Starting direct SFTP connection test")
    
    try:
        # Import necessary modules
        import sys
        sys.path.append('.')
        from utils.sftp import SFTPClient
        from utils.csv_parser import CSVParser
        
        # Load configuration for the Deadside server
        server_id = "c8009f11-4f0f-4c68-8623-dc4b5c393722"
        original_server_id = "7020"
        
        # Server information
        server_info = {
            "hostname": "79.127.236.1",
            "port": 8822,
            "username": "baked",
            "password": "emerald",
            "original_server_id": original_server_id
        }
        
        # Create SFTP client
        logger.info(f"Creating SFTP client for {server_info['hostname']}:{server_info['port']}")
        sftp_client = SFTPClient(
            hostname=server_info["hostname"],
            port=server_info["port"],
            username=server_info["username"],
            password=server_info["password"],
            server_id=server_id,
            original_server_id=original_server_id
        )
        
        # Connect to server
        logger.info("Connecting to SFTP server")
        connected = await sftp_client.connect()
        
        if not connected:
            logger.error("Failed to connect to SFTP server")
            return False
            
        logger.info("Successfully connected to SFTP server")
        
        # Search for CSV files
        deathlogs_path = "/79.127.236.1_7020/actual1/deathlogs"
        
        logger.info(f"Searching for CSV files in {deathlogs_path}")
        csv_files = await sftp_client.find_files(deathlogs_path, pattern=r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}\.csv", recursive=True, max_depth=3)
        
        if not csv_files:
            logger.error("No CSV files found")
            return False
            
        logger.info(f"Found {len(csv_files)} CSV files")
        
        # Display sample of files found
        sample_files = csv_files[:5]
        logger.info(f"Sample files: {sample_files}")
        
        # Create CSV parser
        csv_parser = CSVParser()
        
        # Process a few files
        events = []
        successful_files = 0
        failed_files = 0
        formats_used = {}
        
        # Test with a few recent files
        for file_path in sample_files:
            logger.info(f"Processing file: {file_path}")
            
            try:
                # Download file content
                content = await sftp_client.read_file(file_path)
                
                if not content:
                    logger.warning(f"Empty file: {file_path}")
                    failed_files += 1
                    continue
                    
                # Check the first few lines
                first_lines = content.split('\n')[:5]
                logger.info(f"First few lines: {first_lines}")
                
                # Parse the file
                file_events = csv_parser.parse(content, server_id=server_id)
                
                if not file_events:
                    logger.warning(f"No events parsed from {file_path}")
                    failed_files += 1
                    continue
                    
                # Log timestamps
                for event in file_events[:3]:
                    timestamp = event.get("timestamp")
                    if timestamp:
                        logger.info(f"Parsed timestamp: {timestamp}")
                        
                logger.info(f"Successfully processed {len(file_events)} events from {file_path}")
                events.extend(file_events)
                successful_files += 1
                
            except Exception as e:
                logger.error(f"Error processing {file_path}: {str(e)}")
                failed_files += 1
        
        # Log results
        logger.info(f"Successfully processed {successful_files} files")
        logger.info(f"Failed to process {failed_files} files")
        logger.info(f"Total events processed: {len(events)}")
        
        # Log some event details
        if events:
            sample_event = events[0]
            logger.info(f"Sample event: {json.dumps(sample_event, default=str)}")
            
        # Disconnect
        await sftp_client.disconnect()
        logger.info("Disconnected from SFTP server")
        
        return successful_files > 0
        
    except Exception as e:
        logger.error(f"Error testing SFTP connection: {str(e)}")
        return False

async def main():
    """Main function"""
    try:
        success = await test_sftp_connection()
        
        if success:
            logger.info("SFTP connection test PASSED")
        else:
            logger.error("SFTP connection test FAILED")
            
    except Exception as e:
        logger.error(f"Unhandled error in test: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())