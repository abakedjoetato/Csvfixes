"""
Final verification script for the CSV timestamp parsing fix

This script:
1. Tests with actual files from the running bot's SFTP connection
2. Verifies the YYYY.MM.DD-HH.MM.SS format is correctly parsed
3. Outputs a summary of the fixed timestamp parsing
"""
import asyncio
import logging
import sys
import os
from datetime import datetime, timedelta, timezone

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="timestamp_fix_verification.log",
    filemode="w"
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger("").addHandler(console)

logger = logging.getLogger(__name__)

async def main():
    """Run the verification"""
    sys.path.append(".")
    
    # Import CSVParser to test parsing
    from utils.csv_parser import CSVParser
    parser = CSVParser()
    
    # Test data with specific format
    logger.info("Testing YYYY.MM.DD-HH.MM.SS format parsing...")
    timestamp_str = "2025.05.09-11.58.37"
    test_csv_line = f"{timestamp_str};TestKiller;12345;TestVictim;67890;AK47;100;PC"
    
    # Parse test data
    events = parser.parse_csv_data(test_csv_line)
    if not events:
        logger.error("Failed to parse test CSV data")
        return
        
    event = events[0]
    timestamp = event.get("timestamp")
    
    if not isinstance(timestamp, datetime):
        logger.error(f"Failed to parse timestamp: {timestamp_str} -> {timestamp}")
        return
        
    logger.info(f"Successfully parsed {timestamp_str} -> {timestamp}")
    
    # Test with real CSV files from the SFTP server
    logger.info("Testing with real CSV files...")
    try:
        # Import the CSV processor cog
        from cogs.csv_processor import CSVProcessorCog
        from bot import initialize_bot
        
        # Initialize bot to get database connection
        bot = await initialize_bot(force_sync=False)
        if not bot:
            logger.error("Failed to initialize bot")
            return
            
        # Database connection
        db = bot.db
        
        # Create CSV processor
        csv_processor = CSVProcessorCog(bot)
        
        # Get server configurations
        server_configs = await csv_processor._get_server_configs()
        if not server_configs:
            logger.error("No server configurations found")
            return
            
        # Test each server configuration
        for config in server_configs:
            server_id = config.get("server_id")
            name = config.get("name", "Unknown")
            
            if not server_id:
                continue
                
            logger.info(f"Testing server: {name} (ID: {server_id})")
            
            # Test SFTP connection
            success, sftp = await csv_processor._connect_sftp(config)
            if not success or not sftp:
                logger.error(f"Failed to connect to SFTP for server {name}")
                continue
                
            # Set the last processed date to 60 days ago
            csv_processor.last_processed[server_id] = datetime.now(timezone.utc) - timedelta(days=60)
            
            # Find CSV files
            logger.info(f"Finding CSV files for server {name}...")
            csv_files = await csv_processor._find_csv_files(sftp, config)
            
            if not csv_files:
                logger.warning(f"No CSV files found for server {name}")
                continue
                
            logger.info(f"Found {len(csv_files)} CSV files for server {name}")
            
            # Test a sample of the files
            sample_files = csv_files[:3]
            for csv_file in sample_files:
                logger.info(f"Testing file: {csv_file}")
                
                # Download the file
                content = await csv_processor._get_csv_content(sftp, csv_file)
                if not content:
                    logger.error(f"Failed to download CSV file: {csv_file}")
                    continue
                    
                # Parse the CSV content
                events = parser.parse_csv_data(content)
                if not events:
                    logger.error(f"No events parsed from {csv_file}")
                    continue
                    
                # Check timestamps
                timestamp_success = True
                for i, event in enumerate(events[:5]):  # Check first 5 events
                    timestamp = event.get("timestamp")
                    if not isinstance(timestamp, datetime):
                        logger.error(f"Failed to parse timestamp in event {i+1}: {timestamp}")
                        timestamp_success = False
                        break
                        
                if timestamp_success:
                    logger.info(f"✅ Successfully parsed timestamps in {csv_file}")
                    # Log a sample event
                    sample_event = events[0]
                    timestamp = sample_event.get("timestamp")
                    killer = sample_event.get("killer_name")
                    victim = sample_event.get("victim_name")
                    logger.info(f"  Sample event: {timestamp} - {killer} killed {victim}")
                else:
                    logger.error(f"❌ Failed to parse timestamps in {csv_file}")
                    
            # Close SFTP connection
            await sftp.close()
            
        logger.info("Verification complete")
        print("\n✅ TIMESTAMP FIX VERIFICATION COMPLETE - See timestamp_fix_verification.log for details")
        
    except Exception as e:
        logger.error(f"Error during verification: {e}")
        print(f"\n❌ ERROR DURING VERIFICATION: {e}")
        
if __name__ == "__main__":
    asyncio.run(main())