"""
Test CSV processing improvements with local files.

This script tests our enhanced CSV processing logic with files 
from the attached_assets folder.
"""
import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Import CSV parser and event normalization utilities
from utils.csv_parser import CSVParser
from utils.parser_utils import normalize_event_data, categorize_event

# Test files
TEST_FILES = [
    "attached_assets/2025.03.27-00.00.00.csv",
    "attached_assets/2025.05.01-00.00.00.csv",
    "attached_assets/2025.05.03-00.00.00.csv"
]

async def main():
    """Main test function"""
    logger.info("Testing CSV processing with sample files")
    
    # Create CSV parser
    csv_parser = CSVParser(format_name="deadside", server_id="test-server-id")
    
    # Process each test file
    for file_path in TEST_FILES:
        if not os.path.exists(file_path):
            logger.warning(f"Test file not found: {file_path}")
            continue
            
        logger.info(f"Processing file: {file_path}")
        
        try:
            # Parse the CSV file
            events = csv_parser.parse_csv_file(file_path)
            logger.info(f"Found {len(events)} events in {file_path}")
            
            # Process the events
            kills = 0
            suicides = 0
            unknown = 0
            
            for event in events:
                # Normalize event data
                normalized_event = normalize_event_data(event)
                
                # Skip empty events
                if not normalized_event:
                    continue
                    
                # Categorize event
                event_type = categorize_event(normalized_event)
                
                # Get event details for display
                killer_name = normalized_event.get("killer_name", "Unknown")
                victim_name = normalized_event.get("victim_name", "Unknown")
                weapon = normalized_event.get("weapon", "Unknown")
                distance = normalized_event.get("distance", 0)
                timestamp = normalized_event.get("timestamp", datetime.utcnow())
                
                # Log event based on type
                if event_type == "kill":
                    logger.info(f"Kill event: {killer_name} killed {victim_name} with {weapon} ({distance}m)")
                    kills += 1
                elif event_type == "suicide":
                    logger.info(f"Suicide event: {victim_name} died using {weapon}")
                    suicides += 1
                else:
                    logger.info(f"Unknown event type: {event_type}")
                    unknown += 1
            
            # Log event counts
            logger.info(f"File {file_path} summary:")
            logger.info(f"  - Total events: {len(events)}")
            logger.info(f"  - Kills: {kills}")
            logger.info(f"  - Suicides: {suicides}")
            logger.info(f"  - Unknown: {unknown}")
            logger.info("-" * 40)
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())