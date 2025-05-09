#!/usr/bin/env python3
"""
Comprehensive CSV Processing and SFTP Connection Test

This script tests the fixed SFTP client handling to verify that the entire
CSV processing pipeline works correctly with the modified SFTPManager.connect() method.
"""

import os
import sys
import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('csv_sftp_test.log')
    ]
)
logger = logging.getLogger(__name__)

# Add parent directory to path to import project modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import project modules
from utils.sftp import SFTPManager
from utils.csv_parser import CSVParser
from utils.server_identity import identify_server

async def test_sftp_connect():
    """Test SFTP connection with the corrected connect method"""
    # Create an SFTPManager with test credentials
    manager = SFTPManager(
        hostname="79.127.236.1",
        port=8822,
        username="baked",
        password="emeralds4832",
        server_id="3e6e7113-5b39-4b8c-83bf-a5b4168ba397",
        max_retries=3
    )
    
    # Connect to the SFTP server
    logger.info("Connecting to SFTP server...")
    sftp_manager = await manager.connect()
    
    # Verify that the connect method returned the manager itself
    logger.info(f"Connection successful: {sftp_manager.is_connected}")
    logger.info(f"Manager returned is same instance: {manager is sftp_manager}")
    
    # Verify the corrected methods
    try:
        # Test the listdir method (compatibility method)
        test_path = "/79.127.236.1_7020/actual1/deathlogs/world_0"
        logger.info(f"Testing listdir method on {test_path}...")
        files = await sftp_manager.listdir(test_path)
        logger.info(f"Found {len(files)} files")
        
        # Test file download
        if files:
            sample_file = f"{test_path}/{files[0]}"
            logger.info(f"Testing download_file on {sample_file}...")
            content = await sftp_manager.download_file(sample_file)
            if content:
                logger.info(f"Successfully downloaded {len(content)} bytes")
                
                # Try parsing the CSV file
                logger.info("Testing CSV parsing...")
                csv_lines = content.decode('utf-8').splitlines()
                
                # Create CSV parser
                parser = CSVParser(format_name="deadside", 
                                  hostname="79.127.236.1", 
                                  server_id="7020")
                
                # Parse the CSV data
                events = parser.parse_csv_data(csv_lines)
                logger.info(f"Parsed {len(events)} events from CSV file")
                
                # Show sample events
                for i, event in enumerate(events[:3]):
                    logger.info(f"Event {i+1}: {event}")
    except Exception as e:
        logger.error(f"Error during SFTP operations: {e}")
    
    # Disconnect
    logger.info("Disconnecting from SFTP server...")
    await sftp_manager.disconnect()
    logger.info("Test completed")

async def main():
    """Run all tests"""
    logger.info("Starting comprehensive CSV and SFTP test")
    
    try:
        await test_sftp_connect()
    except Exception as e:
        logger.error(f"Test failed with error: {e}")
    
    logger.info("All tests completed")

if __name__ == "__main__":
    asyncio.run(main())