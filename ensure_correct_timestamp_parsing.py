"""
This script adds a direct timestamp test file to verify that the YYYY.MM.DD-HH.MM.SS format is correctly processed
"""
import os
import logging
import json
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def create_test_csv_file():
    """Create a test CSV file with YYYY.MM.DD-HH.MM.SS format"""
    # Create test directory if it doesn't exist
    test_dir = "attached_assets"
    os.makedirs(test_dir, exist_ok=True)
    
    # Current timestamp in the correct format
    now = datetime.now()
    timestamp = now.strftime("%Y.%m.%d-%H.%M.%S")
    
    # Create CSV file with timestamp format
    csv_path = os.path.join(test_dir, f"{timestamp}.csv")
    
    # CSV content with kill data
    csv_content = f"""{timestamp};TestKiller;12345;TestVictim;67890;AK47;100;PC
{timestamp};Player1;11111;Player2;22222;M4;200;PC
{timestamp};Player3;33333;Player4;44444;Pistol;50;PC"""
    
    # Write CSV file
    with open(csv_path, 'w') as f:
        f.write(csv_content)
        
    logger.info(f"Created test CSV file: {csv_path}")
    return csv_path

def create_command_file():
    """Create a command file that tells the bot to process the CSV files"""
    command = {
        "action": "process_csv",
        "target_channel_id": 1360632422957449237,
        "timestamp": datetime.now().isoformat(),
        "message": "🔄 Processing CSV files with fixed timestamp format (YYYY.MM.DD-HH.MM.SS)..."
    }
    
    # Write command file
    command_path = "csv_process_command.json"
    with open(command_path, 'w') as f:
        json.dump(command, f)
        
    logger.info(f"Created command file: {command_path}")
    return command_path

def main():
    """Main function to create test files"""
    logger.info("Creating test files to verify YYYY.MM.DD-HH.MM.SS format parsing")
    
    # Create test CSV file
    csv_path = create_test_csv_file()
    
    # Create command file
    command_path = create_command_file()
    
    # Create a test marker file that indicates a special test is running
    marker_path = ".test_timestamp_format"
    with open(marker_path, 'w') as f:
        f.write(f"Test started at {datetime.now().isoformat()}\n")
        f.write(f"Test CSV file: {csv_path}\n")
        f.write(f"Command file: {command_path}\n")
        
    logger.info(f"Created marker file: {marker_path}")
    logger.info("Test setup complete. The bot should detect and process the files.")
    logger.info("Check Discord channel for results.")
    
if __name__ == "__main__":
    main()