"""
Direct test of CSV batch processing

This script tests CSV batch processing directly by:
1. Creating a new server config with proper timestamp format support
2. Running the processing function with a 60-day lookback
3. Verifying events are properly inserted into the database

This will verify the timestamp parsing fix is working as part of complete processing.
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta
import re
import json
import traceback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("csv_test_results_v2.log")
    ]
)

logger = logging.getLogger(__name__)

# Constants
SERVER_ID = "c8009f11-4f0f-4c68-8623-dc4b5c393722"
NUMERIC_ID = "7020"  # Used for path construction

async def test_csv_batch_processing():
    """Run a comprehensive test of CSV batch processing"""
    logger.info("Testing CSV batch processing")
    
    results = {
        "success": False,
        "files_found": 0,
        "files_processed": 0,
        "events_processed": 0,
        "errors": [],
        "warnings": [],
        "server_id_resolution": False,
        "csv_file_access": False,
        "timestamp_parsing": False,
        "database_insertion": False
    }
    
    try:
        # Import necessary modules
        sys.path.append('.')
        from utils.server_identity import get_numeric_id, KNOWN_SERVERS
        from utils.sftp import SFTPManager
        from utils.csv_parser import CSVParser
        
        # Check server ID resolution
        try:
            if SERVER_ID in KNOWN_SERVERS:
                mapped_id = KNOWN_SERVERS.get(SERVER_ID)
                logger.info(f"Server {SERVER_ID} mapped to {mapped_id} in KNOWN_SERVERS")
                results["server_id_resolution"] = mapped_id == NUMERIC_ID
            else:
                logger.error(f"Server ID {SERVER_ID} not found in KNOWN_SERVERS")
                results["errors"].append("Server ID not found in KNOWN_SERVERS")
                results["server_id_resolution"] = False
        except Exception as e:
            logger.error(f"Error checking server ID resolution: {str(e)}")
            results["errors"].append(f"Server ID resolution error: {str(e)}")
            results["server_id_resolution"] = False
        
        # Check CSV file access
        try:
            # Server config
            config = {
                "hostname": "79.127.236.1",
                "port": 8822,
                "username": "baked",
                "password": "emerald",
                "sftp_path": "/logs",
                "original_server_id": NUMERIC_ID
            }
            
            # Create SFTP client
            sftp = SFTPManager(
                server_id=SERVER_ID,
                hostname=config["hostname"],
                port=config["port"],
                username=config["username"],
                password=config["password"],
                original_server_id=config["original_server_id"]
            )
            
            # Connect
            await sftp.connect()
            
            # Find CSV files
            csv_pattern = r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}\.csv"
            
            # Standard paths
            paths = [
                f"/79.127.236.1_{NUMERIC_ID}/actual1/deathlogs/world_0",
                f"/79.127.236.1_{NUMERIC_ID}/actual1/deathlogs",
            ]
            
            # Find files
            found_files = []
            for path in paths:
                try:
                    files = await sftp.find_files(path, pattern=csv_pattern)
                    if files:
                        found_files.extend(files)
                        logger.info(f"Found {len(files)} CSV files in {path}")
                except Exception as e:
                    logger.error(f"Error searching path {path}: {str(e)}")
            
            # Close connection
            await sftp.disconnect()
            
            results["files_found"] = len(found_files)
            results["csv_file_access"] = len(found_files) > 0
            
            if not found_files:
                logger.error("No CSV files found")
                results["errors"].append("No CSV files found")
            else:
                logger.info(f"Found {len(found_files)} CSV files")
                
                # Log sample files
                sample_files = found_files[:5]
                for i, file in enumerate(sample_files):
                    logger.info(f"Sample file {i+1}: {file}")
                
        except Exception as e:
            logger.error(f"Error checking CSV file access: {str(e)}")
            results["errors"].append(f"CSV file access error: {str(e)}")
            results["csv_file_access"] = False
        
        # Test timestamp parsing
        if results["csv_file_access"] and found_files:
            try:
                # Create parser
                parser = CSVParser()
                
                # Test timestamp parsing with sample file
                sample_file = found_files[0]
                
                # Create SFTP client again
                sftp = SFTPManager(
                    server_id=SERVER_ID,
                    hostname=config["hostname"],
                    port=config["port"],
                    username=config["username"],
                    password=config["password"],
                    original_server_id=config["original_server_id"]
                )
                
                # Connect
                await sftp.connect()
                
                # Download file
                content = await sftp.read_file(sample_file)
                
                # Close connection
                await sftp.disconnect()
                
                if not content:
                    logger.error(f"Empty content for file {sample_file}")
                    results["errors"].append(f"Empty content for file {sample_file}")
                    results["timestamp_parsing"] = False
                else:
                    # Parse content
                    events = parser.parse_csv_data(content)
                    
                    if not events:
                        logger.error(f"No events parsed from file {sample_file}")
                        results["errors"].append(f"No events parsed from file {sample_file}")
                        results["timestamp_parsing"] = False
                    else:
                        # Check timestamps
                        all_valid = True
                        for i, event in enumerate(events[:5]):  # Check first 5 events
                            timestamp = event.get("timestamp")
                            if not isinstance(timestamp, datetime):
                                logger.error(f"Invalid timestamp in event {i+1}: {timestamp} (type: {type(timestamp).__name__})")
                                all_valid = False
                                break
                            logger.info(f"Event {i+1} timestamp: {timestamp} (type: {type(timestamp).__name__})")
                        
                        results["timestamp_parsing"] = all_valid
                        
                        if all_valid:
                            logger.info("All sample events have valid timestamps")
                        else:
                            results["errors"].append("Invalid timestamps in parsed events")
                
            except Exception as e:
                logger.error(f"Error testing timestamp parsing: {str(e)}")
                results["errors"].append(f"Timestamp parsing error: {str(e)}")
                results["timestamp_parsing"] = False
        
        # Test complete processing
        logger.info("Testing complete CSV processing with CSV processor cog")
        
        # Initialize bot to get CSV processor
        from bot import initialize_bot
        
        bot = await initialize_bot(force_sync=False)
        if not bot:
            logger.error("Failed to initialize bot")
            results["errors"].append("Failed to initialize bot")
        else:
            # Get CSV processor cog
            csv_processor = bot.get_cog("CSVProcessorCog")
            if not csv_processor:
                logger.error("CSV processor cog not found")
                results["errors"].append("CSV processor cog not found")
            else:
                # Create server config
                server_config = {
                    "hostname": "79.127.236.1",
                    "port": 8822,
                    "username": "baked",
                    "password": "emerald",
                    "sftp_path": "/logs",
                    "csv_pattern": r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}\.csv",
                    "original_server_id": NUMERIC_ID,
                }
                
                # Set a cutoff date 60 days ago
                cutoff_date = datetime.now() - timedelta(days=60)
                
                # If the cog has a last_processed dictionary, set the entry for our server
                if hasattr(csv_processor, 'last_processed'):
                    csv_processor.last_processed[SERVER_ID] = cutoff_date
                    logger.info(f"Set processing cutoff date to {cutoff_date}")
                
                # Process the server's CSV files
                try:
                    # Count existing events
                    db = bot.db
                    existing_count = 0
                    if db:
                        existing_count = await db.kills.count_documents({"server_id": SERVER_ID})
                        logger.info(f"Found {existing_count} existing events in database")
                    
                    # Process files
                    start_time = datetime.now()
                    files_processed, events_processed = await csv_processor._process_server_csv_files(
                        SERVER_ID, server_config
                    )
                    duration = (datetime.now() - start_time).total_seconds()
                    
                    logger.info(f"Processed {files_processed} files with {events_processed} events in {duration:.2f} seconds")
                    
                    results["files_processed"] = files_processed
                    results["events_processed"] = events_processed
                    
                    # Check database
                    if db:
                        new_count = await db.kills.count_documents({"server_id": SERVER_ID})
                        logger.info(f"Now have {new_count} events in database (added {new_count - existing_count})")
                        
                        # Get a few sample events
                        cursor = db.kills.find({"server_id": SERVER_ID}).sort("timestamp", -1).limit(5)
                        recent_events = []
                        async for doc in cursor:
                            recent_events.append(doc)
                        
                        if recent_events:
                            logger.info("Sample events from database:")
                            for i, event in enumerate(recent_events):
                                timestamp = event.get("timestamp")
                                formatted_time = timestamp.strftime("%Y-%m-%d %H:%M:%S") if isinstance(timestamp, datetime) else str(timestamp)
                                
                                killer = event.get("killer_name", "Unknown")
                                victim = event.get("victim_name", "Unknown")
                                weapon = event.get("weapon", "Unknown")
                                
                                logger.info(f"Event {i+1}: {formatted_time} - {killer} killed {victim} with {weapon}")
                            
                            results["database_insertion"] = True
                        else:
                            logger.error("No events found in database")
                            results["errors"].append("No events found in database")
                            results["database_insertion"] = False
                    
                except Exception as e:
                    logger.error(f"Error processing server CSV files: {str(e)}")
                    logger.error(traceback.format_exc())
                    results["errors"].append(f"CSV processing error: {str(e)}")
            
            # Close bot
            try:
                await bot.close()
            except:
                pass
        
        # Set overall success
        results["success"] = (
            results["server_id_resolution"] and
            results["csv_file_access"] and
            results["timestamp_parsing"] and
            results["database_insertion"] and
            results["files_processed"] > 0 and
            results["events_processed"] > 0 and
            not results["errors"]
        )
        
        # Print summary
        if results["success"]:
            logger.info("✓ CSV batch processing test PASSED")
            logger.info(f"✓ Successfully processed {results['files_processed']} files with {results['events_processed']} events")
            logger.info("✓ YYYY.MM.DD-HH.MM.SS timestamp format is working correctly")
        else:
            logger.error("✗ CSV batch processing test FAILED")
            for error in results["errors"]:
                logger.error(f"✗ {error}")
        
        return results
        
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        logger.error(traceback.format_exc())
        results["errors"].append(f"Unhandled exception: {str(e)}")
        results["success"] = False
        return results

async def main():
    """Main function"""
    results = await test_csv_batch_processing()
    
    if results["success"]:
        logger.info("\n\n==================================================")
        logger.info("             CSV PROCESSING TEST PASSED              ")
        logger.info("==================================================\n")
        logger.info(f"Files found:       {results['files_found']}")
        logger.info(f"Files processed:   {results['files_processed']}")
        logger.info(f"Events processed:  {results['events_processed']}")
        logger.info("\nAll tests passed successfully!")
    else:
        logger.error("\n\n==================================================")
        logger.error("             CSV PROCESSING TEST FAILED              ")
        logger.error("==================================================\n")
        logger.error(f"Files found:       {results['files_found']}")
        logger.error(f"Files processed:   {results['files_processed']}")
        logger.error(f"Events processed:  {results['events_processed']}")
        logger.error("\nErrors encountered:")
        for error in results["errors"]:
            logger.error(f" - {error}")
            
        if results["warnings"]:
            logger.warning("\nWarnings:")
            for warning in results["warnings"]:
                logger.warning(f" - {warning}")

if __name__ == "__main__":
    asyncio.run(main())