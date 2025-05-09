#!/usr/bin/env python3
"""
Script to directly test the CSV processing using local test files.
This script:
1. Creates a simple mock for direct testing of the CSV parser
2. Tests both full file processing and only-new-lines processing
3. Demonstrates the difference between historical and killfeed modes
"""

import asyncio
import io
import logging
import sys
from datetime import datetime, timedelta
import os
import json

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(stream=sys.stdout)
    ]
)
logger = logging.getLogger('test_csv_local')

# Check if test files exist
test_dir = "./attached_assets"
if os.path.exists(test_dir):
    csv_files = [f for f in os.listdir(test_dir) if f.endswith('.csv')]
    if csv_files:
        logger.info(f"Found {len(csv_files)} CSV test files: {', '.join(csv_files)}")
    else:
        logger.warning("No CSV test files found in ./attached_assets")
else:
    logger.warning("./attached_assets directory not found")

def get_test_file_path():
    """Get the path to the newest test CSV file"""
    test_files = [os.path.join(test_dir, f) for f in os.listdir(test_dir) if f.endswith('.csv')]
    if not test_files:
        return None
    # Sort by filename which should place newest date last
    return sorted(test_files)[-1]

async def test_csv_parser():
    """Test the CSV parser directly"""
    logger.info("Testing CSV parser directly")
    
    # Import the CSV parser
    try:
        from utils.csv_parser import CSVParser
        from utils.parser_utils import normalize_event_data
        logger.info("Successfully imported CSV parser")
    except ImportError as e:
        logger.error(f"Failed to import CSV parser: {e}")
        return
    
    # Create a CSV parser instance
    csv_parser = CSVParser()
    logger.info("Created CSV parser instance")
    
    # Get the test file path
    file_path = get_test_file_path()
    if not file_path:
        logger.error("No test CSV files found")
        return
    
    logger.info(f"Using test file: {file_path}")
    
    # First run: Parse the entire file
    logger.info("TEST 1: Parsing entire file (historical mode)")
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()
    
    events1 = csv_parser.parse_csv_data(content)
    logger.info(f"Parsed {len(events1)} events from full file")
    
    if events1:
        # Print first 2 events
        logger.info(f"First events: {json.dumps(events1[:2], default=str)}")
    
    # Second run: Parse the same file again, should also process all events
    # since using parse_csv_data doesn't track the file position
    logger.info("TEST 2: Parsing entire file again with parse_csv_data")
    events2 = csv_parser.parse_csv_data(content)
    logger.info(f"Parsed {len(events2)} events from full file again")
    
    if len(events1) != len(events2):
        logger.error(f"Expected equal number of events, got {len(events1)} != {len(events2)}")
    
    # Third run: Use the _parse_csv_file method with only_new_lines=True
    # First time should still process all lines because it's tracking a new file
    logger.info("TEST 3: First run with only_new_lines=True")
    content_io = io.StringIO(content)
    events3 = csv_parser._parse_csv_file(content_io, file_path=file_path, only_new_lines=True)
    logger.info(f"Parsed {len(events3)} events with only_new_lines=True (first run)")
    
    # Fourth run: Use the _parse_csv_file method with only_new_lines=True again
    # This time it should skip all lines since we already processed them
    logger.info("TEST 4: Second run with only_new_lines=True (should find no new lines)")
    content_io = io.StringIO(content)
    events4 = csv_parser._parse_csv_file(content_io, file_path=file_path, only_new_lines=True)
    logger.info(f"Parsed {len(events4)} events with only_new_lines=True (second run)")
    
    if events4:
        logger.warning("Expected no events in second run with only_new_lines=True")
    else:
        logger.info("SUCCESS: No events were processed in second run with only_new_lines=True")
    
    # Fifth run: Add a new line to the file and test again
    logger.info("TEST 5: Adding a new line and testing only_new_lines=True")
    additional_line = "2025.05.09-08.00.00;Player1;123456;Player2;654321;Weapon;100.0\n"
    combined_content = content + additional_line
    content_io = io.StringIO(combined_content)
    events5 = csv_parser._parse_csv_file(content_io, file_path=file_path, only_new_lines=True)
    logger.info(f"Parsed {len(events5)} events with only_new_lines=True after adding a new line")
    
    if len(events5) != 1:
        logger.error(f"Expected exactly 1 new event, got {len(events5)}")
    else:
        logger.info("SUCCESS: Only the new line was processed in the updated file")
        logger.info(f"New event: {json.dumps(events5[0], default=str)}")
    
    logger.info("CSV parser test completed")

async def main():
    logger.info("Starting CSV processing test with local files")
    
    # Test the CSV parser directly
    await test_csv_parser()
    
    logger.info("Test completed")

if __name__ == "__main__":
    asyncio.run(main())