"""
Advanced diagnostic script for CSV processing

This script provides a comprehensive diagnostic for CSV file processing:
1. Tests local file parsing
2. Tests SFTP file discovery and download
3. Tests event processing pipeline including kill/suicide categorization
4. Validates database storage
"""
import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import json

# Configure enhanced logging with timestamp
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('csv_diagnostic.log')
    ]
)
logger = logging.getLogger('csv_diagnostic')

# Target server - adjust as needed
TARGET_SERVER_ID = "2eb00d14-8b8c-4371-8f75-facfe10f86cb"

# Status trackers
discovered_files = []
downloaded_files = []
processed_files = []
processed_kills = 0
processed_suicides = 0
database_failures = 0

# Test files
TEST_FILES = [
    "attached_assets/2025.03.27-00.00.00.csv",
    "attached_assets/2025.05.01-00.00.00.csv",
    "attached_assets/2025.05.03-00.00.00.csv"
]

async def test_local_parsing():
    """Test parsing of local CSV files"""
    logger.info("=== Testing Local CSV Parsing ===")
    try:
        # Import CSV parser
        from utils.csv_parser import CSVParser
        from utils.parser_utils import normalize_event_data, categorize_event, detect_suicide
        
        parser = CSVParser(format_name="deadside", server_id="test-server")
        
        for file_path in TEST_FILES:
            if not os.path.exists(file_path):
                logger.warning(f"Local test file not found: {file_path}")
                continue
                
            logger.info(f"Parsing file: {file_path}")
            
            try:
                # Parse CSV file
                events = parser.parse_csv_file(file_path)
                logger.info(f"Found {len(events)} raw events in file {file_path}")
                
                if not events:
                    logger.error(f"No events parsed from file {file_path} - parser returned empty list")
                    continue
                
                # Categorize events to test event pipeline
                kills = 0
                suicides = 0
                unknown = 0
                
                # Process a sample of the events (max 10)
                sample_events = events[:min(10, len(events))]
                logger.info(f"Processing sample of {len(sample_events)} events")
                
                for event in sample_events:
                    normalized = normalize_event_data(event)
                    if not normalized:
                        logger.warning(f"Failed to normalize event: {event}")
                        continue
                        
                    event_type = categorize_event(normalized)
                    is_suicide = detect_suicide(normalized)
                    
                    # Extract info for logging
                    timestamp = normalized.get('timestamp', 'Unknown')
                    killer_name = normalized.get('killer_name', 'Unknown')
                    victim_name = normalized.get('victim_name', 'Unknown')
                    weapon = normalized.get('weapon', 'Unknown')
                    
                    if event_type == 'kill':
                        logger.info(f"Kill: {killer_name} -> {victim_name} with {weapon}")
                        kills += 1
                    elif event_type == 'suicide':
                        logger.info(f"Suicide: {victim_name} with {weapon}")
                        suicides += 1
                    else:
                        logger.info(f"Unknown event type: {event_type}")
                        unknown += 1
                
                # Log stats for this file
                logger.info(f"File {file_path} sample stats: {kills} kills, {suicides} suicides, {unknown} unknown")
                
            except Exception as e:
                logger.error(f"Error processing file {file_path}: {e}", exc_info=True)
        
        return True
    except Exception as e:
        logger.error(f"Local parsing test failed: {e}", exc_info=True)
        return False

async def test_sftp_connection():
    """Test SFTP connection and file discovery"""
    logger.info("=== Testing SFTP Connection ===")
    try:
        # Get server config
        from utils.database import get_db
        
        # Initialize database
        db = await get_db()
        if not db:
            logger.error("Failed to connect to database")
            return False
            
        # Get server configuration
        server = await db.game_servers.find_one({"server_id": TARGET_SERVER_ID})
        if not server:
            logger.error(f"Server {TARGET_SERVER_ID} not found in database")
            return False
            
        # Extract SFTP configuration
        sftp_config = {
            "hostname": server.get("hostname"),
            "port": server.get("port", 22),
            "username": server.get("username"),
            "password": server.get("password"),
            "sftp_path": server.get("sftp_path", "/logs"),
            "original_server_id": server.get("original_server_id")
        }
        
        # Log SFTP configuration (without password)
        safe_config = sftp_config.copy()
        safe_config["password"] = "******" if "password" in safe_config else None
        logger.info(f"SFTP Configuration: {json.dumps(safe_config, default=str)}")
        
        # Create SFTP manager
        from utils.sftp import SFTPManager
        
        # Remove sftp_path from config as it's not a valid parameter
        if 'sftp_path' in sftp_config:
            del sftp_config['sftp_path']
            
        sftp = SFTPManager(server_id=TARGET_SERVER_ID, **sftp_config)
        logger.info(f"Created SFTP manager for {sftp_config['hostname']}:{sftp_config['port']}")
        
        # Connect to SFTP
        logger.info("Connecting to SFTP server...")
        connected = await sftp.connect()
        if not connected:
            logger.error("Failed to connect to SFTP server")
            return False
            
        logger.info("Successfully connected to SFTP server")
        
        # Find CSV files
        logger.info("Looking for CSV files...")
        csv_files = await sftp.find_csv_files(max_depth=10)
        
        if not csv_files:
            logger.error("No CSV files found on SFTP server")
            return False
            
        # Record found files
        global discovered_files
        discovered_files = csv_files[:10]  # Store first 10 for reference
        
        logger.info(f"Found {len(csv_files)} CSV files, first 10: {discovered_files}")
        
        # Test downloading a sample file
        if csv_files:
            sample_file = csv_files[0]
            logger.info(f"Testing download of {sample_file}")
            
            try:
                # Use sftp.get_file method instead
                file_size, remote_data = await sftp.get_file(sample_file)
                
                if not remote_data:
                    logger.error(f"Failed to download {sample_file}")
                else:
                    # Record success
                    global downloaded_files
                    downloaded_files.append(sample_file)
                    
                    logger.info(f"Successfully downloaded {sample_file} ({file_size} bytes)")
                    
                    # Show a sample of the content
                    if isinstance(remote_data, bytes):
                        try:
                            sample = remote_data[:200].decode('utf-8')
                            logger.info(f"Content sample: {sample}")
                        except:
                            logger.error("Failed to decode content as UTF-8")
            except Exception as e:
                logger.error(f"Error downloading file: {e}")
                
        # Disconnect
        await sftp.disconnect()
        logger.info("Disconnected from SFTP server")
        
        return True
    except Exception as e:
        logger.error(f"SFTP connection test failed: {e}", exc_info=True)
        return False

async def test_historical_parsing():
    """Test the historical parsing functionality"""
    logger.info("=== Testing Historical Parsing ===")
    try:
        # Create bot instance
        from bot import initialize_bot
        
        # Initialize the bot
        bot = await initialize_bot(force_sync=False)
        
        if not bot or not bot.db:
            logger.error("Failed to initialize bot or database connection")
            return False
            
        logger.info("Bot initialized with database connection")
        
        # Get the CSV processor cog
        csv_processor = bot.get_cog('CSVProcessorCog')
        if not csv_processor:
            logger.error("Failed to get CSVProcessorCog")
            return False
            
        logger.info("Retrieved CSVProcessorCog")
        
        # Try running historical parse with a 1-day window to minimize processing time
        logger.info(f"Running historical parse for {TARGET_SERVER_ID} with 1-day window")
        
        # Skip server config retrieval - we'll use run_historical_parse directly
        
        # Use standard historical parse method
        files_processed, events_processed = await csv_processor.run_historical_parse(
            TARGET_SERVER_ID, days=1
        )
        
        global processed_files, processed_kills, processed_suicides
        processed_files = files_processed
        
        # Check database for processed events
        kill_count = await bot.db.kills.count_documents({
            "server_id": TARGET_SERVER_ID,
            "timestamp": {"$gte": datetime.now() - timedelta(days=1)}
        })
        
        suicide_count = await bot.db.kills.count_documents({
            "server_id": TARGET_SERVER_ID,
            "timestamp": {"$gte": datetime.now() - timedelta(days=1)},
            "is_suicide": True
        })
        
        processed_kills = kill_count - suicide_count
        processed_suicides = suicide_count
        
        logger.info(f"Historical parse results:")
        logger.info(f"  - Files processed: {files_processed}")
        logger.info(f"  - Events processed: {events_processed}")
        logger.info(f"  - Database kill records: {kill_count}")
        logger.info(f"  - Regular kills: {processed_kills}")
        logger.info(f"  - Suicides: {processed_suicides}")
        
        return files_processed > 0
    except Exception as e:
        logger.error(f"Historical parsing test failed: {e}", exc_info=True)
        return False

async def main():
    """Run all diagnostic tests"""
    logger.info("Starting comprehensive CSV diagnostic tests")
    
    # Test local parsing
    local_parsing_ok = await test_local_parsing()
    logger.info(f"Local parsing test: {'PASSED' if local_parsing_ok else 'FAILED'}")
    
    # Test SFTP connection
    sftp_ok = await test_sftp_connection()
    logger.info(f"SFTP connection test: {'PASSED' if sftp_ok else 'FAILED'}")
    
    # Test historical parsing
    historical_ok = await test_historical_parsing()
    logger.info(f"Historical parsing test: {'PASSED' if historical_ok else 'FAILED'}")
    
    # Overall diagnostic summary
    logger.info("=== Diagnostic Summary ===")
    logger.info(f"Local parsing: {'PASSED' if local_parsing_ok else 'FAILED'}")
    logger.info(f"SFTP connection: {'PASSED' if sftp_ok else 'FAILED'}")
    logger.info(f"Historical parsing: {'PASSED' if historical_ok else 'FAILED'}")
    logger.info(f"Discovered files: {len(discovered_files)}")
    logger.info(f"Downloaded files: {len(downloaded_files)}")
    logger.info(f"Processed files: {processed_files}")
    logger.info(f"Processed kills: {processed_kills}")
    logger.info(f"Processed suicides: {processed_suicides}")
    
    # Final verdict
    if local_parsing_ok and sftp_ok and historical_ok:
        logger.info("OVERALL VERDICT: All tests PASSED - CSV processing is working correctly")
    else:
        logger.error("OVERALL VERDICT: Some tests FAILED - CSV processing has issues")
        
        # Provide troubleshooting guidance
        if not local_parsing_ok:
            logger.error("Local parsing failed - check CSV parser for bugs")
        if not sftp_ok:
            logger.error("SFTP failed - check connection parameters or network issues")
        if not historical_ok:
            logger.error("Historical parsing failed - check bot configuration and database connection")

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())