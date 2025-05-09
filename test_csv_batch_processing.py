"""
Test CSV Batch Processing

This script directly verifies the CSV file processing for the fix.
"""

import asyncio
import os
import csv
import io
import logging
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, Set

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('csv_test_results_v2.log')
    ]
)

logger = logging.getLogger(__name__)

async def test_csv_processing():
    """Test CSV file processing with timestamp parsing fix"""
    logger.info("Starting CSV processing test...")
    
    # 1. First test local CSV files in the attached_assets directory
    csv_files = []
    if os.path.exists("attached_assets"):
        csv_files = [
            os.path.join("attached_assets", f) 
            for f in os.listdir("attached_assets") 
            if f.endswith(".csv")
        ]
    
    if not csv_files:
        logger.error("No CSV files found in attached_assets directory")
        return False
    
    # Result stats
    total_files = len(csv_files)
    successful_files = 0
    failed_files = 0
    total_events = 0
    unique_players = set()
    formats_used = {
        "%Y.%m.%d-%H.%M.%S": 0,
        "%Y.%m.%d-%H:%M:%S": 0,
        "%Y-%m-%d-%H.%M.%S": 0,
        "%Y-%m-%d %H:%M:%S": 0,
        "%Y.%m.%d %H:%M:%S": 0,
        "other": 0
    }
    
    logger.info(f"Found {len(csv_files)} CSV files to test")
    
    # Process each file
    for file_path in csv_files:
        try:
            logger.info(f"Processing file: {file_path}")
            
            # Check if file exists and has content
            if not os.path.exists(file_path):
                logger.error(f"File does not exist: {file_path}")
                failed_files += 1
                continue
                
            if os.path.getsize(file_path) == 0:
                logger.warning(f"Empty file: {file_path}")
                failed_files += 1
                continue
            
            # Read file content
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
            
            if not content.strip():
                logger.warning(f"File contains only whitespace: {file_path}")
                failed_files += 1
                continue
            
            # Auto-detect delimiter
            semicolons = content.count(';')
            commas = content.count(',')
            delimiter = ';' if semicolons > commas else ','
            logger.info(f"Using delimiter: '{delimiter}' for {file_path}")
            
            # Parse CSV
            content_io = io.StringIO(content)
            reader = csv.reader(content_io, delimiter=delimiter)
            
            events = []
            file_formats_used = {}
            
            for row in reader:
                if len(row) < 6:  # Skip rows with insufficient fields
                    continue
                
                # Extract timestamp
                timestamp_str = row[0].strip() if row[0] else None
                if not timestamp_str:
                    continue
                
                # Try to parse timestamp with multiple formats
                timestamp = None
                format_used = None
                
                for date_format in [
                    "%Y.%m.%d-%H.%M.%S",  # Primary format: 2025.05.03-00.00.00
                    "%Y.%m.%d-%H:%M:%S",  # With colons
                    "%Y-%m-%d-%H.%M.%S",  # With dashes
                    "%Y-%m-%d %H:%M:%S",  # Standard format
                    "%Y.%m.%d %H:%M:%S",  # Dots for date
                ]:
                    try:
                        timestamp = datetime.strptime(timestamp_str, date_format)
                        format_used = date_format
                        formats_used[date_format] = formats_used.get(date_format, 0) + 1
                        file_formats_used[date_format] = file_formats_used.get(date_format, 0) + 1
                        break
                    except ValueError:
                        continue
                
                if not timestamp:
                    logger.warning(f"Could not parse timestamp: {timestamp_str}")
                    formats_used["other"] = formats_used.get("other", 0) + 1
                    continue
                
                # Extract event data
                killer_name = row[1].strip() if len(row) > 1 and row[1] else "Unknown"
                killer_id = row[2].strip() if len(row) > 2 and row[2] else "Unknown"
                victim_name = row[3].strip() if len(row) > 3 and row[3] else "Unknown"
                victim_id = row[4].strip() if len(row) > 4 and row[4] else "Unknown"
                weapon = row[5].strip() if len(row) > 5 and row[5] else "Unknown"
                
                # Collect unique players
                if killer_id and killer_id != "Unknown":
                    unique_players.add(killer_id)
                if victim_id and victim_id != "Unknown":
                    unique_players.add(victim_id)
                
                # Create event
                event = {
                    "timestamp": timestamp,
                    "killer_name": killer_name,
                    "killer_id": killer_id,
                    "victim_name": victim_name,
                    "victim_id": victim_id,
                    "weapon": weapon,
                    "format_used": format_used
                }
                
                events.append(event)
            
            # Record results
            event_count = len(events)
            if event_count > 0:
                successful_files += 1
                total_events += event_count
                logger.info(f"Successfully processed {event_count} events from {file_path}")
                if file_formats_used:
                    for fmt, count in file_formats_used.items():
                        logger.info(f"  Format {fmt}: {count} events")
            else:
                failed_files += 1
                logger.warning(f"No events found in {file_path}")
                
        except Exception as e:
            failed_files += 1
            logger.error(f"Error processing {file_path}: {str(e)}")
    
    # 2. Test date comparison logic
    logger.info("\nTesting date comparison logic for timestamp cutoffs")
    now = datetime.now()
    cutoff_times = [
        ("24 hours", now - timedelta(days=1)),
        ("7 days", now - timedelta(days=7)),
        ("30 days", now - timedelta(days=30)),
        ("60 days", now - timedelta(days=60)),
    ]
    
    test_dates = [
        ("1 hour ago", now - timedelta(hours=1)),
        ("1 day ago", now - timedelta(days=1)),
        ("7 days ago", now - timedelta(days=7)),
        ("30 days ago", now - timedelta(days=30)),
        ("59 days ago", now - timedelta(days=59)),
        ("61 days ago", now - timedelta(days=61)),
    ]
    
    for cutoff_name, cutoff_date in cutoff_times:
        logger.info(f"\nCutoff: {cutoff_name} ({cutoff_date})")
        for test_name, test_date in test_dates:
            result = test_date > cutoff_date
            logger.info(f"  {test_name} ({test_date}) > cutoff? {result}")
    
    # 3. Summary
    success_rate = (successful_files / total_files) * 100 if total_files > 0 else 0
    logger.info("\n===== CSV PROCESSING TEST SUMMARY =====")
    logger.info(f"Total files tested: {total_files}")
    logger.info(f"Successfully processed files: {successful_files} ({success_rate:.1f}%)")
    logger.info(f"Failed files: {failed_files}")
    logger.info(f"Total events extracted: {total_events}")
    logger.info(f"Unique players found: {len(unique_players)}")
    logger.info("\nFormat usage:")
    for fmt, count in formats_used.items():
        logger.info(f"  {fmt}: {count} events")
    
    logger.info("===== TEST COMPLETE =====")
    
    return successful_files > 0 and total_events > 0

async def main():
    """Main function"""
    try:
        logger.info("Starting CSV batch processing test")
        success = await test_csv_processing()
        
        if success:
            logger.info("CSV processing test PASSED")
        else:
            logger.error("CSV processing test FAILED")
            
        logger.info("Test complete")
        
    except Exception as e:
        logger.error(f"Error running test: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())