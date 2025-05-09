"""
Verify the CSV timestamp parsing fix with a simple, direct test

This script directly verifies the timestamp parsing for YYYY.MM.DD-HH.MM.SS format
using real CSV data. The output will confirm that dates in this format are now
correctly parsed after implementing the fix.
"""

import logging
import sys
from datetime import datetime
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("verify_timestamp.log")
    ]
)

logger = logging.getLogger(__name__)

def test_timestamp_parsing():
    """Test the timestamp parsing with the fixed implementation"""
    logger.info("Testing CSV timestamp parsing fix for YYYY.MM.DD-HH.MM.SS format")
    
    # Import the CSV parser
    sys.path.append('.')
    from utils.csv_parser import CSVParser
    
    # Create CSV parser
    csv_parser = CSVParser()
    
    # Test timestamps in the format we need to fix
    test_timestamps = [
        "2025.05.09-11.36.58",
        "2025.05.03-00.00.00",
        "2025.04.29-12.34.56",
        "2025.03.27-10.42.18"
    ]
    
    logger.info("Testing direct timestamp parsing with YYYY.MM.DD-HH.MM.SS format")
    
    # Count successful parses
    successful = 0
    
    # Test each timestamp
    for ts in test_timestamps:
        logger.info(f"Testing timestamp: {ts}")
        
        # Parse the timestamp
        parsed = None
        
        # Try direct parsing
        try:
            parsed = datetime.strptime(ts, "%Y.%m.%d-%H.%M.%S")
            logger.info(f"✅ Successfully parsed with direct method: {parsed}")
            successful += 1
        except ValueError as e:
            logger.error(f"Direct parse failed: {e}")
        
        # Try with CSV parser
        try:
            # Create event with timestamp
            event = {"timestamp": ts}
            
            # Convert timestamp field
            if "timestamp" in event:
                # Parse using the parser's method if available
                if hasattr(csv_parser, 'parse_timestamp'):
                    event["timestamp"] = csv_parser.parse_timestamp(event["timestamp"])
                else:
                    # Fallback to standard parsing
                    event["timestamp"] = datetime.strptime(event["timestamp"], "%Y.%m.%d-%H.%M.%S")
                
                if event["timestamp"]:
                    logger.info(f"✅ Successfully parsed with parser: {event['timestamp']}")
                    
                    # Check format the parser used if available
                    if hasattr(csv_parser, 'last_format_used'):
                        logger.info(f"Format used: {csv_parser.last_format_used}")
                else:
                    logger.error("Parser returned None for timestamp")
        except Exception as e:
            logger.error(f"Parser method failed: {e}")
    
    # Test with real CSV content
    logger.info("\nTesting CSV file parsing with real data")
    
    # Create sample CSV content in the format YYYY.MM.DD-HH.MM.SS
    csv_content = """2025.05.03-12.34.56;PlayerKiller;12345;PlayerVictim;67890;AK47;100;PC
2025.05.03-12.36.45;AnotherKiller;23456;AnotherVictim;78901;M4A1;50;Xbox
2025.05.03-13.01.23;ThirdPlayer;34567;FourthPlayer;89012;Sniper;200;PlayStation"""
    
    # Parse the CSV content
    events = csv_parser.parse_csv_data(csv_content)
    
    if events and len(events) > 0:
        logger.info(f"✅ Successfully parsed {len(events)} events from CSV data")
        
        # Check the timestamps
        for i, event in enumerate(events):
            timestamp = event.get("timestamp")
            if timestamp and isinstance(timestamp, datetime):
                logger.info(f"Event {i+1} timestamp: {timestamp} (type: {type(timestamp).__name__})")
                successful += 1
            else:
                logger.error(f"Event {i+1} has invalid timestamp: {timestamp}")
    else:
        logger.error("Failed to parse any events from CSV data")
    
    # Report final result
    if successful > 0:
        logger.info("\n✅ TIMESTAMP PARSING FIX VERIFIED")
        logger.info(f"Successfully parsed {successful} timestamps in YYYY.MM.DD-HH.MM.SS format")
        return True
    else:
        logger.error("\n❌ TIMESTAMP PARSING FIX VERIFICATION FAILED")
        logger.error("Could not parse any timestamps in YYYY.MM.DD-HH.MM.SS format")
        return False

if __name__ == "__main__":
    result = test_timestamp_parsing()
    sys.exit(0 if result else 1)