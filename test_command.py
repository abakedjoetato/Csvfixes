"""
Simple script to directly test CSV file processing using local test files.

This script will process sample CSV files from the attached_assets folder
to verify event categorization and suicide detection without requiring SFTP.
"""
import asyncio
import logging
import os
import sys
from datetime import datetime
from typing import Dict, Any, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('csv_test.log')
    ]
)
logger = logging.getLogger('test_csv')

# Import utilities
from utils.csv_parser import CSVParser
from utils.parser_utils import normalize_event_data, categorize_event, detect_suicide

# Test files in attached_assets
TEST_FILES = [
    'attached_assets/2025.03.27-00.00.00.csv',
    'attached_assets/2025.05.01-00.00.00.csv',
    'attached_assets/2025.05.03-00.00.00.csv',
]

async def process_test_files():
    """Process test CSV files and categorize events"""
    # Create parser
    parser = CSVParser(format_name="deadside", server_id="test-server")
    
    # Process stats
    total_files = 0
    total_events = 0
    total_kills = 0
    total_suicides = 0
    total_unknown = 0
    
    # Process each file
    for file_path in TEST_FILES:
        if not os.path.exists(file_path):
            logger.warning(f"Test file not found: {file_path}")
            continue
            
        logger.info(f"Processing file: {file_path}")
        
        try:
            # Parse CSV file
            events = parser.parse_csv_file(file_path)
            logger.info(f"Found {len(events)} events in file")
            
            # Categorize events
            kills = 0
            suicides = 0
            unknown = 0
            
            # Process events
            for event in events:
                # Normalize event data
                normalized = normalize_event_data(event)
                if not normalized:
                    continue
                    
                # Categorize event
                event_type = categorize_event(normalized)
                
                # Extract event details for logging
                timestamp = normalized.get('timestamp', datetime.now())
                killer_name = normalized.get('killer_name', 'Unknown')
                victim_name = normalized.get('victim_name', 'Unknown')
                weapon = normalized.get('weapon', 'Unknown')
                distance = normalized.get('distance', 0)
                
                # Log detailed event information
                if event_type == 'kill':
                    logger.info(f"Kill event: {killer_name} killed {victim_name} with {weapon} ({distance}m)")
                    kills += 1
                elif event_type == 'suicide':
                    logger.info(f"Suicide event: {victim_name} died using {weapon}")
                    suicides += 1
                else:
                    logger.info(f"Unknown event: {event_type} - {normalized.get('id', 'unknown')}")
                    unknown += 1
            
            # Update totals
            total_files += 1
            total_events += len(events)
            total_kills += kills
            total_suicides += suicides
            total_unknown += unknown
            
            # Log file summary
            logger.info(f"File summary for {file_path}:")
            logger.info(f"  - Total events: {len(events)}")
            logger.info(f"  - Kills: {kills}")
            logger.info(f"  - Suicides: {suicides}")
            logger.info(f"  - Unknown: {unknown}")
            logger.info("-" * 40)
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}", exc_info=True)
    
    # Log overall stats
    logger.info("=" * 40)
    logger.info("CSV Processing Complete")
    logger.info(f"Total files processed: {total_files}")
    logger.info(f"Total events processed: {total_events}")
    logger.info(f"Total kills: {total_kills}")
    logger.info(f"Total suicides: {total_suicides}")
    logger.info(f"Total unknown events: {total_unknown}")
    logger.info("=" * 40)

if __name__ == "__main__":
    # Run the async function
    asyncio.run(process_test_files())