"""
Final script to test the CSV timestamp parsing fix directly.

This script:
1. Creates a test CSV file with the YYYY.MM.DD-HH.MM.SS format
2. Uses the CSVParser directly to test parsing
3. Verifies timestamps are correctly parsed
4. Logs results in detail
"""
import asyncio
import logging
import os
import sys
from datetime import datetime
from typing import List, Dict, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="fix_timestamp_direct.log",
    filemode="w"
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger("").addHandler(console)

logger = logging.getLogger(__name__)

def create_test_csv_file() -> str:
    """Create a test CSV file with YYYY.MM.DD-HH.MM.SS format
    
    Returns:
        str: Path to created CSV file
    """
    # Create test directory
    test_dir = "attached_assets"
    os.makedirs(test_dir, exist_ok=True)
    
    # Current time in the YYYY.MM.DD-HH.MM.SS format
    now = datetime.now()
    timestamp = now.strftime("%Y.%m.%d-%H.%M.%S")
    
    # Create CSV file path
    csv_path = os.path.join(test_dir, f"{timestamp}.csv")
    
    # CSV content with the test timestamp
    csv_content = f"""{timestamp};TestKiller;12345;TestVictim;67890;AK47;100;PC
{timestamp};Player1;11111;Player2;22222;M4;200;PC
{timestamp};Player3;33333;Player4;44444;Pistol;50;PC"""
    
    # Write CSV file
    with open(csv_path, "w") as f:
        f.write(csv_content)
        
    logger.info(f"Created test CSV file: {csv_path}")
    return csv_path

async def test_csv_parsing(csv_path: str) -> bool:
    """Test CSV parsing with the CSVParser
    
    Args:
        csv_path: Path to test CSV file
        
    Returns:
        bool: True if parsing successful, False otherwise
    """
    # Import CSVParser class
    sys.path.append(".")
    from utils.csv_parser import CSVParser
    
    # Create parser
    parser = CSVParser()
    
    # Read CSV file
    with open(csv_path, "r") as f:
        csv_data = f.read()
        
    # Parse CSV data
    logger.info(f"Parsing CSV data from {csv_path}")
    events = parser.parse_csv_data(csv_data)
    
    # Check results
    if not events:
        logger.error("No events parsed from CSV data")
        return False
        
    # Check first event
    event = events[0]
    timestamp = event.get("timestamp")
    
    if not isinstance(timestamp, datetime):
        logger.error(f"Failed to parse timestamp: {timestamp} - not a datetime object")
        return False
        
    # All timestamps should match
    logger.info(f"Successfully parsed timestamp: {timestamp}")
    
    # Log all events
    for i, event in enumerate(events):
        logger.info(f"Event {i+1}:")
        for key, value in event.items():
            logger.info(f"  {key}: {value}")
            
    return True

async def main():
    """Main function"""
    logger.info("Starting CSV timestamp format fix verification")
    
    # Create test CSV file
    csv_path = create_test_csv_file()
    
    # Test CSV parsing
    success = await test_csv_parsing(csv_path)
    
    if success:
        logger.info("✅ CSV timestamp parsing works correctly with YYYY.MM.DD-HH.MM.SS format")
        print("\n✅ TIMESTAMP FORMAT FIX VERIFIED - See fix_timestamp_direct.log for details")
    else:
        logger.error("❌ CSV timestamp parsing failed with YYYY.MM.DD-HH.MM.SS format")
        print("\n❌ TIMESTAMP FORMAT FIX FAILED - See fix_timestamp_direct.log for details")
        
if __name__ == "__main__":
    asyncio.run(main())