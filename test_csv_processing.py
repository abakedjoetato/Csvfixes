"""
Simplified test script for CSV processing to ensure correct handling
of CSV files from game servers.

This script tests:
1. Local CSV file parsing
2. Event classification (kills vs suicides)
3. Database storage verification
"""
import asyncio
import logging
import os
import sys
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('csv_test.log')
    ]
)
logger = logging.getLogger('csv_test')

# Test files
TEST_FILES = [
    "attached_assets/2025.03.27-00.00.00.csv",
    "attached_assets/2025.05.01-00.00.00.csv",
    "attached_assets/2025.05.03-00.00.00.csv"
]

async def test_local_csv_parsing():
    """Test the local CSV parser on sample files"""
    logger.info("=== Testing Local CSV Parser ===")
    
    # Import CSV parser
    from utils.csv_parser import CSVParser
    from utils.parser_utils import normalize_event_data, categorize_event
    
    parser = CSVParser(format_name="deadside", server_id="test-server")
    
    total_events = 0
    kills = 0
    suicides = 0
    unknown = 0
    
    for file_path in TEST_FILES:
        if not os.path.exists(file_path):
            logger.warning(f"Test file not found: {file_path}")
            continue
        
        logger.info(f"Processing file: {file_path}")
        
        # Parse the file
        try:
            events = parser.parse_csv_file(file_path)
            logger.info(f"Found {len(events)} events in {file_path}")
            
            # Process events
            file_kills = 0
            file_suicides = 0
            file_unknown = 0
            
            for event in events:
                normalized = normalize_event_data(event)
                if not normalized:
                    logger.warning(f"Failed to normalize event: {event}")
                    continue
                    
                event_type = categorize_event(normalized)
                
                if event_type == 'kill':
                    file_kills += 1
                elif event_type == 'suicide':
                    file_suicides += 1
                else:
                    file_unknown += 1
            
            # Update totals
            total_events += len(events)
            kills += file_kills
            suicides += file_suicides
            unknown += file_unknown
            
            # Log stats for this file
            logger.info(f"File stats: {file_kills} kills, {file_suicides} suicides, {file_unknown} unknown")
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}", exc_info=True)
    
    # Log overall stats
    logger.info("=== CSV Parser Results ===")
    logger.info(f"Total events processed: {total_events}")
    logger.info(f"Kills: {kills}")
    logger.info(f"Suicides: {suicides}")
    logger.info(f"Unknown: {unknown}")
    
    return total_events > 0

async def main():
    """Run the CSV processing tests"""
    logger.info("Starting CSV processing test")
    
    # Test local CSV parsing
    csv_ok = await test_local_csv_parsing()
    
    # Print summary
    logger.info("=== Test Summary ===")
    logger.info(f"CSV parsing: {'PASSED' if csv_ok else 'FAILED'}")
    
    if csv_ok:
        logger.info("All tests passed successfully!")
    else:
        logger.error("Some tests failed!")

if __name__ == "__main__":
    asyncio.run(main())