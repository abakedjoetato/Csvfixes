"""
Direct test of timestamp parsing with the format YYYY.MM.DD-HH.MM.SS
"""

import logging
from datetime import datetime
from utils.csv_parser import CSVParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

def test_timestamp_parsing():
    """Test timestamp parsing with the format YYYY.MM.DD-HH.MM.SS"""
    # Create CSV parser
    parser = CSVParser()
    
    # Test timestamps
    test_timestamps = [
        "2025.05.09-11.36.58",
        "2025.05.03-00.00.00",
        "2025.04.29-12.34.56",
        "2025.03.27-10.42.18"
    ]
    
    logger.info("Testing timestamp parsing with format YYYY.MM.DD-HH.MM.SS")
    
    # Test each timestamp with raw CSV data
    for ts in test_timestamps:
        # Create CSV data with the timestamp
        csv_data = f"{ts};PlayerKiller;12345;PlayerVictim;67890;AK47;100;PC"
        
        # Parse the CSV data
        events = parser.parse_csv_data(csv_data)
        
        if not events:
            logger.error(f"Failed to parse event for timestamp {ts}")
            continue
            
        event = events[0]
        timestamp = event.get("timestamp")
        
        if not isinstance(timestamp, datetime):
            logger.error(f"❌ Timestamp {ts} not converted to datetime: {timestamp}")
        else:
            logger.info(f"✅ Successfully parsed {ts} to {timestamp}")

if __name__ == "__main__":
    test_timestamp_parsing()