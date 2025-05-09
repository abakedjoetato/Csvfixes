#!/usr/bin/env python3
"""
Test script for CSV processing functionality

This script:
1. Sets up a minimal environment to test the CSV processor
2. Uses direct file access to local CSV samples to ensure parsing works
3. Includes enhanced logging and error output
4. Directly processes test CSV files without SFTP dependencies
"""
import asyncio
import json
import logging
import os
import re
import sys
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("csv_test.log")
    ]
)
logger = logging.getLogger(__name__)

# Test CSV files
TEST_CSV_FILES = [
    "attached_assets/2025.03.27-00.00.00.csv",
    "attached_assets/2025.05.01-00.00.00.csv",
    "attached_assets/2025.05.03-00.00.00.csv",
    "attached_assets/2025.05.03-01.00.00.csv",
    "attached_assets/2025.05.03-02.00.00.csv",
    "attached_assets/2025.05.03-03.00.00.csv"
]

async def parse_csv_file(file_path):
    """Parse a single CSV file and return events"""
    logger.info(f"Parsing CSV file: {file_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            logger.info(f"Successfully read file {file_path} ({len(content)} bytes)")
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        return []
    
    # Try different delimiters
    delimiters = [';', ',', '\t', '|']
    events = []
    
    for delimiter in delimiters:
        try:
            logger.info(f"Trying delimiter: '{delimiter}'")
            lines = content.splitlines()
            
            if not lines:
                logger.warning(f"No lines found in file {file_path}")
                continue
                
            # Sample first line to check if delimiter works
            sample_line = lines[0]
            fields = sample_line.split(delimiter)
            
            if len(fields) >= 5:  # Expect at least 5 fields for a valid CSV row
                logger.info(f"Found valid delimiter: '{delimiter}' with {len(fields)} fields")
                
                # Process all lines with this delimiter
                for line in lines:
                    if not line.strip():
                        continue
                        
                    try:
                        parts = line.split(delimiter)
                        if len(parts) < 5:
                            logger.warning(f"Skipping line with insufficient fields: {line}")
                            continue
                            
                        # Extract fields - format: time,killer_name,killer_id,victim_name,victim_id,weapon,distance
                        timestamp_str = parts[0].strip()
                        killer_name = parts[1].strip() if len(parts) > 1 else ""
                        killer_id = parts[2].strip() if len(parts) > 2 else ""
                        victim_name = parts[3].strip() if len(parts) > 3 else ""
                        victim_id = parts[4].strip() if len(parts) > 4 else ""
                        weapon = parts[5].strip() if len(parts) > 5 else ""
                        distance = parts[6].strip() if len(parts) > 6 else "0"
                        
                        # Normalize timestamp
                        timestamp = None
                        for fmt in ["%Y-%m-%d %H:%M:%S", "%Y.%m.%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%d.%m.%Y %H:%M:%S"]:
                            try:
                                timestamp = datetime.strptime(timestamp_str, fmt)
                                break
                            except ValueError:
                                continue
                        
                        if not timestamp:
                            logger.warning(f"Could not parse timestamp: {timestamp_str}")
                            # Use file date as fallback
                            match = re.search(r'(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2})', file_path)
                            if match:
                                try:
                                    timestamp = datetime.strptime(match.group(1), "%Y.%m.%d-%H.%M.%S")
                                except ValueError:
                                    timestamp = datetime.now()
                            else:
                                timestamp = datetime.now()
                        
                        # Detect suicide event
                        is_suicide = False
                        if killer_id == victim_id and killer_id:
                            is_suicide = True
                            logger.info(f"Detected suicide by ID match: {killer_name} == {victim_name}")
                        elif weapon and weapon.lower() in ["falling", "suicide_by_relocation", "suicide"]:
                            is_suicide = True
                            logger.info(f"Detected suicide by weapon: {weapon}")
                        elif killer_name == victim_name and killer_name:
                            is_suicide = True
                            logger.info(f"Detected suicide by name match: {killer_name}")
                            
                        # Extract distance as float when possible
                        try:
                            distance_value = float(distance.replace("m", "").strip())
                        except (ValueError, TypeError):
                            distance_value = 0
                            
                        # Create event object
                        event = {
                            "timestamp": timestamp,
                            "killer_name": killer_name,
                            "killer_id": killer_id,
                            "victim_name": victim_name,
                            "victim_id": victim_id,
                            "weapon": weapon,
                            "distance": distance_value,
                            "is_suicide": is_suicide,
                            "event_type": "suicide" if is_suicide else "kill",
                            "server_id": "test_server",
                            "source_file": os.path.basename(file_path)
                        }
                        
                        events.append(event)
                        
                    except Exception as e:
                        logger.error(f"Error processing line: {line}: {e}")
                
                # If we found valid events, stop trying other delimiters
                if events:
                    break
        except Exception as e:
            logger.error(f"Error processing file with delimiter '{delimiter}': {e}")
    
    logger.info(f"Extracted {len(events)} events from {file_path}")
    return events

async def main():
    """Run CSV processing test"""
    logger.info(f"Starting CSV processing test with {len(TEST_CSV_FILES)} test files")
    
    all_events = []
    files_processed = 0
    
    # Process each test file
    for file_path in TEST_CSV_FILES:
        if not os.path.exists(file_path):
            logger.error(f"Test file not found: {file_path}")
            continue
            
        events = await parse_csv_file(file_path)
        all_events.extend(events)
        files_processed += 1
    
    # Analyze results
    kills = [e for e in all_events if not e["is_suicide"]]
    suicides = [e for e in all_events if e["is_suicide"]]
    
    # Group by weapon
    weapons = {}
    for event in all_events:
        weapon = event["weapon"]
        if weapon not in weapons:
            weapons[weapon] = 0
        weapons[weapon] += 1
    
    # Group by server
    servers = {}
    for event in all_events:
        server_id = event["server_id"]
        if server_id not in servers:
            servers[server_id] = 0
        servers[server_id] += 1
    
    # Print summary
    logger.info(f"CSV processing complete: {files_processed} files processed")
    logger.info(f"Total events: {len(all_events)}")
    logger.info(f"Kills: {len(kills)}")
    logger.info(f"Suicides: {len(suicides)}")
    logger.info(f"Unique weapons: {len(weapons)}")
    logger.info(f"Top weapons: {sorted(weapons.items(), key=lambda x: x[1], reverse=True)[:5]}")
    logger.info(f"Servers: {servers}")
    
    # Dump first few events for inspection
    if all_events:
        sample = all_events[:5]
        for i, event in enumerate(sample):
            # Convert datetime to string for JSON serialization
            event_copy = event.copy()
            event_copy["timestamp"] = event_copy["timestamp"].isoformat()
            logger.info(f"Sample event {i+1}: {json.dumps(event_copy, indent=2)}")
    
if __name__ == "__main__":
    asyncio.run(main())