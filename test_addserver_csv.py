"""
Test Add Server CSV Processing

This script directly tests the CSV processing functionality used by the /addserver command
to verify that our delimiter fix works properly in that context.
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('addserver_csv_test.log', mode='w')
    ]
)
logger = logging.getLogger("addserver_csv_test")

# Emulate server configuration used by /addserver
TEST_SERVER_CONFIG = {
    "hostname": "79.127.236.1",
    "port": 8822,
    "username": "baked",
    "password": "emerald",
    "server_id": "c8009f11-4f0f-4c68-8623-dc4b5c393722",
    "original_server_id": "7020",
    "server_name": "Emerald Test Server",
    "base_path": "/79.127.236.1_7020/actual1/deathlogs/world_0",
    "sftp_enabled": True
}

async def test_csv_parser_directly():
    """Test the CSV parser with the delimiter parameter directly"""
    try:
        from utils.csv_parser import CSVParser
        
        # Create test data with semicolon delimiter (matching the production format)
        test_data = """2025.05.09-11.58.37;Player1;ID1;Player2;ID2;weapon;10;PS4;PS4;
2025.05.09-12.01.22;Player3;ID3;Player4;ID4;weapon2;15;PC;PC;"""
        
        # Create parser and test
        parser = CSVParser()
        events = parser.parse_csv_data(test_data, delimiter=';')
        
        # Check results
        logger.info(f"CSV Parser test: Successfully parsed {len(events)} events with delimiter")
        return len(events) > 0
    except Exception as e:
        logger.error(f"CSV Parser test error: {e}")
        return False

async def simulate_historical_parse():
    """Simulate the historical parse process used by /addserver"""
    try:
        # Import required utilities
        from utils.sftp import SFTPClient

        # SFTP settings from test server config
        hostname = TEST_SERVER_CONFIG["hostname"]
        port = TEST_SERVER_CONFIG["port"]
        username = TEST_SERVER_CONFIG["username"]
        password = TEST_SERVER_CONFIG["password"]
        server_id = TEST_SERVER_CONFIG["server_id"]
        original_server_id = TEST_SERVER_CONFIG["original_server_id"]
        base_path = TEST_SERVER_CONFIG["base_path"]
        
        logger.info(f"Connecting to SFTP server: {hostname}:{port}")
        
        # Connect to SFTP
        sftp = SFTPClient(
            hostname=hostname,
            port=port,
            username=username,
            password=password,
            server_id=server_id,
            original_server_id=original_server_id
        )
        
        await sftp.connect()
        logger.info("Connected to SFTP server")
        
        # Check if base path exists
        try:
            await sftp.listdir(base_path)
            logger.info(f"Successfully accessed base path: {base_path}")
        except Exception as e:
            logger.error(f"Failed to access base path: {e}")
            await sftp.close()
            return False
        
        # Find CSV files
        csv_files = []
        files = await sftp.listdir(base_path)
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(base_path, file))
        
        logger.info(f"Found {len(csv_files)} CSV files in {base_path}")
        
        if not csv_files:
            logger.error("No CSV files found")
            await sftp.close()
            return False
        
        # Test download and parse one file
        test_file = csv_files[0]
        logger.info(f"Testing file: {test_file}")
        
        # Download file content
        content = await sftp.download_file(test_file)
        if not content:
            logger.error(f"Failed to download {test_file}")
            await sftp.close()
            return False
        
        logger.info(f"Downloaded {len(content)} bytes from {test_file}")
        
        # Parse the file with our fixed parser
        from utils.csv_parser import CSVParser
        parser = CSVParser()
        
        # This is where our fix should apply - passing the delimiter parameter
        events = parser.parse_csv_data(content, delimiter=';')
        
        logger.info(f"Successfully parsed {len(events)} events from {test_file}")
        
        # Close connection
        await sftp.close()
        
        return len(events) > 0
    except Exception as e:
        logger.error(f"Historical parse simulation error: {e}")
        import traceback
        traceback.print_exc()
        return False
        
async def test_emergency_fix_code():
    """Test the emergency fix code that deals with the delimiter parameter"""
    try:
        # Create test input that emulates what happens in the emergency fix code
        from utils.csv_parser import CSVParser
        from io import StringIO
        
        test_data = """2025.05.09-11.58.37;Player1;ID1;Player2;ID2;weapon;10;PS4;PS4;
2025.05.09-12.01.22;Player3;ID3;Player4;ID4;weapon2;15;PC;PC;"""
        
        # Create parser
        parser = CSVParser()
        
        # Create the content IO object
        content_io = StringIO(test_data)
        
        # Emulate the emergency fix code that was failing
        detected_delimiter = ";"
        
        # This is the line that was failing:
        # events = self.csv_parser._parse_csv_file(content_io, file_path=file_path, only_new_lines=False, delimiter=detected_delimiter)
        events = parser._parse_csv_file(content_io, file_path="test.csv", only_new_lines=False, delimiter=detected_delimiter)
        
        logger.info(f"Emergency fix code test: Successfully parsed {len(events)} events")
        
        return len(events) > 0
    except Exception as e:
        logger.error(f"Emergency fix code test error: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """Run all tests"""
    logger.info("Starting Add Server CSV Handling Tests")
    
    # Test 1: Basic parser test
    parser_success = await test_csv_parser_directly()
    logger.info(f"CSV Parser direct test: {'PASSED' if parser_success else 'FAILED'}")
    
    # Test 2: Emergency fix code test
    emergency_success = await test_emergency_fix_code()
    logger.info(f"Emergency fix code test: {'PASSED' if emergency_success else 'FAILED'}")
    
    # Test 3: Historical parse simulation
    historical_success = await simulate_historical_parse()
    logger.info(f"Historical parse simulation: {'PASSED' if historical_success else 'FAILED'}")
    
    # Overall result
    if parser_success and emergency_success and historical_success:
        logger.info("All CSV tests PASSED - The /addserver command should work correctly")
        print("\n✅ All tests PASSED - The /addserver command should now work correctly with CSV files\n")
    else:
        logger.error("Some tests FAILED")
        print("\n❌ Some tests FAILED - Check addserver_csv_test.log for details\n")

if __name__ == "__main__":
    asyncio.run(main())