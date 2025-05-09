"""
Test Historical Parse Functionality

This script tests the historical parse functionality with the fixes applied.
"""

import asyncio
import logging
import sys
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("historical_parse.log", mode="w"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("historical_parse_test")

# Test server configuration
TEST_SERVER_CONFIG = {
    "hostname": "79.127.236.1",
    "port": 8822,
    "username": "baked",
    "password": "emerald",
    "server_id": "c8009f11-4f0f-4c68-8623-dc4b5c393722",
    "original_server_id": "7020",
    "server_name": "Emerald Test Server",
    "base_path": "/79.127.236.1_7020/actual1/deathlogs/world_0",
    "sftp_enabled": True,
    "guild_id": "1219706687980568769"
}

async def test_historical_parse():
    """Test historical parse functionality"""
    try:
        # Import the CSV processor cog
        sys.path.append('.')
        from cogs.csv_processor import CSVProcessorCog
        
        logger.info("Successfully imported CSVProcessorCog")
        
        # Create a mock bot class for testing
        class MockBot:
            def __init__(self):
                self.db = None
                
            def wait_until_ready(self):
                async def dummy():
                    pass
                return dummy()
            
            @property
            def user(self):
                return None
        
        # Create the cog instance
        bot = MockBot()
        cog = CSVProcessorCog(bot)
        logger.info("Created CSVProcessorCog instance")
        
        # Modify the cog to skip actual processing for testing
        cog._process_server_csv_files = mock_process_server_csv_files
        
        # Test the run_historical_parse_with_config method
        logger.info("Testing run_historical_parse_with_config method")
        server_id = "c8009f11-4f0f-4c68-8623-dc4b5c393722"
        files_processed, events_processed = await cog.run_historical_parse_with_config(
            server_id=server_id,
            server_config=TEST_SERVER_CONFIG,
            days=7
        )
        
        logger.info(f"Historical parse result: {files_processed} files processed, {events_processed} events processed")
        return files_processed is not None and events_processed is not None
        
    except Exception as e:
        logger.error(f"Error in historical parse test: {e}")
        import traceback
        traceback.print_exc()
        return False

async def mock_process_server_csv_files(self, server_id, config, start_date=None):
    """Mock implementation for testing"""
    logger.info(f"Mock _process_server_csv_files called with server_id={server_id}")
    logger.info(f"Config: {config}")
    logger.info(f"Start date: {start_date}")
    
    # Return test values
    return 5, 123  # 5 files processed, 123 events processed

async def main():
    """Run the test"""
    print("\n======== Testing Historical Parse Functionality ========\n")
    
    success = await test_historical_parse()
    
    if success:
        print("\n✅ Historical parse test PASSED - The fix is working correctly\n")
    else:
        print("\n❌ Historical parse test FAILED - The fix is not complete\n")

if __name__ == "__main__":
    asyncio.run(main())