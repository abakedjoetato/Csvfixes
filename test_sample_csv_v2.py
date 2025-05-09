"""
Enhanced CSV Processing Test with correct timestamp format

This script directly processes a sample CSV file from attached_assets to verify
the CSV parsing functionality with the proper timestamp format.
"""

import asyncio
import os
import csv
import re
import io
from datetime import datetime
import sys
import logging

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


async def process_csv_file(file_path):
    """Process a CSV file and extract kill events"""
    logger.info(f"Processing file: {file_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()
            logger.info(f"Successfully read file: {file_path} ({len(content)} bytes)")
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {str(e)}")
        return 0, []
    
    if not content:
        logger.warning(f"Empty content read from {file_path}")
        return 0, []
    
    # Get sample of the content
    sample = content[:200] + "..." if len(content) > 200 else content
    logger.info(f"Content sample: {sample}")
    
    # Detect delimiter
    semicolon_count = content.count(';')
    comma_count = content.count(',')
    tab_count = content.count('\t')
    
    logger.info(f"Delimiter detection: semicolons={semicolon_count}, commas={comma_count}, tabs={tab_count}")
    
    # Determine the most likely delimiter
    detected_delimiter = ';'  # Default for our format
    if comma_count > semicolon_count and comma_count > tab_count:
        detected_delimiter = ','
    elif tab_count > semicolon_count and tab_count > comma_count:
        detected_delimiter = '\t'
    
    logger.info(f"Using detected delimiter: '{detected_delimiter}' for file {file_path}")
    
    # Try to parse CSV content
    events = []
    content_io = io.StringIO(content)
    
    try:
        reader = csv.reader(content_io, delimiter=detected_delimiter)
        for i, row in enumerate(reader):
            # Skip empty rows
            if not row:
                continue
                
            # Log the first few rows to understand structure
            if i < 5:
                logger.info(f"Row {i+1}: {row}")
            
            # If we have enough fields for a kill event
            if len(row) >= 6:
                try:
                    # Extract timestamp
                    timestamp_str = row[0].strip() if row[0] else None
                    if timestamp_str:
                        try:
                            # Try to parse timestamp with the correct format: YYYY.MM.DD-HH.MM.SS
                            timestamp = datetime.strptime(timestamp_str, "%Y.%m.%d-%H.%M.%S")
                            logger.info(f"Successfully parsed timestamp: {timestamp_str} -> {timestamp}")
                        except Exception as e:
                            # If format fails, try alternative formats
                            timestamp = None
                            for fmt in ["%Y-%m-%d %H:%M:%S", "%Y.%m.%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"]:
                                try:
                                    timestamp = datetime.strptime(timestamp_str, fmt)
                                    break
                                except ValueError:
                                    continue
                                    
                            if not timestamp:
                                # If all formats failed, use the current time
                                timestamp = datetime.now()
                                logger.warning(f"Could not parse timestamp: {timestamp_str}, using current time")
                    else:
                        timestamp = datetime.now()
                        
                    # Extract other fields with fallbacks
                    killer_name = row[1].strip() if len(row) > 1 and row[1] else "Unknown"
                    killer_id = row[2].strip() if len(row) > 2 and row[2] else "Unknown"
                    victim_name = row[3].strip() if len(row) > 3 and row[3] else "Unknown"
                    victim_id = row[4].strip() if len(row) > 4 and row[4] else "Unknown"
                    weapon = row[5].strip() if len(row) > 5 and row[5] else "Unknown"
                    
                    # Extract distance if available
                    distance = 0
                    if len(row) > 6 and row[6]:
                        try:
                            distance = float(row[6].strip())
                        except (ValueError, TypeError):
                            distance = 0
                    
                    # Create event dictionary
                    event = {
                        "timestamp": timestamp,
                        "killer_name": killer_name,
                        "killer_id": killer_id,
                        "victim_name": victim_name,
                        "victim_id": victim_id,
                        "weapon": weapon,
                        "distance": distance
                    }
                    
                    # Only add if we have both killer and victim IDs
                    if killer_id and victim_id:
                        events.append(event)
                    else:
                        logger.warning(f"Skipping row {i+1} due to missing killer_id or victim_id")
                        
                except Exception as e:
                    logger.error(f"Error processing row {i+1}: {e}")
            else:
                logger.warning(f"Row {i+1} has insufficient fields: {len(row)}")
    
    except Exception as e:
        logger.error(f"Error parsing CSV: {str(e)}")
    
    logger.info(f"Processed {len(events)} events from file {file_path}")
    
    # Print a few sample events
    for i, event in enumerate(events[:5]):
        logger.info(f"Event {i+1}: {event}")
    
    return len(events), events


async def main():
    """Main function"""
    logger.info("Starting direct CSV processing test with correct timestamp format")
    
    # First, check if we have any CSV files in attached_assets
    assets_dir = "attached_assets"
    if not os.path.exists(assets_dir):
        logger.error(f"Directory does not exist: {assets_dir}")
        return
        
    # Find CSV files
    csv_files = [os.path.join(assets_dir, f) for f in os.listdir(assets_dir) 
                 if f.endswith('.csv') and os.path.isfile(os.path.join(assets_dir, f))]
    
    logger.info(f"Found {len(csv_files)} CSV files in {assets_dir}")
    
    if not csv_files:
        logger.error("No CSV files found in attached_assets directory")
        return
    
    # Process each file
    total_events = 0
    for file_path in csv_files:
        events_count, _ = await process_csv_file(file_path)
        total_events += events_count
        
    logger.info(f"Total events processed from all files: {total_events}")


if __name__ == "__main__":
    asyncio.run(main())