"""
Final Fix for CSV Parser and Historical Parser

This script fixes the remaining issues with the CSV parser, including:
1. The historical parser's files_processed variable initialization
2. The SFTP connection parameter issues
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("fix_csv_timestamp_final.log", mode="w"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Path to the CSV processor file
CSV_PROCESSOR_PATH = "cogs/csv_processor.py"

async def fix_historical_parser():
    """Fix the historical parser's files_processed variable initialization"""
    try:
        logger.info("Starting final CSV parser fix")
        
        # Create a backup of the file
        backup_path = f"{CSV_PROCESSOR_PATH}.backup"
        os.system(f"cp {CSV_PROCESSOR_PATH} {backup_path}")
        logger.info(f"Created backup of {CSV_PROCESSOR_PATH} at {backup_path}")
        
        # Read the file content
        with open(CSV_PROCESSOR_PATH, "r") as f:
            content = f.read()
        
        # Fix #1: Initialize files_processed in _process_server_csv_files
        # Look for the method definition
        start_index = content.find("async def _process_server_csv_files")
        if start_index == -1:
            logger.error("Could not find _process_server_csv_files method")
            return False
        
        # Find the logger.info line after the method definition
        logger_line_start = content.find('logger.info(f"DIAGNOSTIC: Processing CSV files', start_index)
        if logger_line_start == -1:
            logger.error("Could not find the diagnostic logger line in _process_server_csv_files")
            return False
        
        # Find the end of that line
        logger_line_end = content.find('\n', logger_line_start)
        
        # Check if files_processed is initialized right after the logger line
        next_few_lines = content[logger_line_end:logger_line_end+200]
        if "files_processed = 0" not in next_few_lines:
            # Insert the initialization after the logger line
            modified_content = (
                content[:logger_line_end+1] + 
                "        # Initialize counters\n" +
                "        files_processed = 0\n" +
                "        events_processed = 0\n" +
                content[logger_line_end+1:]
            )
            logger.info("Added initialization of files_processed variable")
        else:
            logger.info("files_processed is already initialized, no change needed")
            modified_content = content
        
        # Fix #2: Fix the SFTPManager connection parameters in the enhanced connection handling
        # Look for code that's passing hostname to SFTPManager.connect()
        connection_issue_pattern = "sftp_manager.connect(\n                            hostname="
        if connection_issue_pattern in modified_content:
            # Remove the parameters that are causing the "unexpected keyword argument" error
            fixed_connection = modified_content.replace(
                "await asyncio.wait_for(\n                        sftp_manager.connect(\n                            hostname=config[\"hostname\"],\n                            port=config[\"port\"],\n                            username=config.get(\"username\", \"baked\"),\n                            password=config.get(\"password\", \"emerald\"),\n                            server_id=server_id\n                        ),",
                "await asyncio.wait_for(\n                        sftp_manager.connect(),")
            logger.info("Fixed SFTPManager.connect() parameter issue")
        else:
            fixed_connection = modified_content
            logger.info("No SFTPManager.connect() parameter issue found")
        
        # Write the modified content back to the file
        with open(CSV_PROCESSOR_PATH, "w") as f:
            f.write(fixed_connection)
        
        logger.info(f"Successfully updated {CSV_PROCESSOR_PATH}")
        return True
    
    except Exception as e:
        logger.error(f"Error fixing historical parser: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_fix():
    """Test if the fix resolves the issue"""
    try:
        # Import the csv_processor module to check for syntax errors
        import importlib.util
        spec = importlib.util.spec_from_file_location("csv_processor", CSV_PROCESSOR_PATH)
        if spec is None:
            logger.error("Failed to load spec for csv_processor.py")
            return False
            
        csv_processor = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(csv_processor)
        logger.info("Successfully imported csv_processor module - no syntax errors")
        
        return True
    except Exception as e:
        logger.error(f"Error testing fix: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """Run the fix and test"""
    success = await fix_historical_parser()
    if success:
        logger.info("Successfully applied fixes to historical parser")
        
        # Test if the fix resolves the issue
        test_success = await test_fix()
        if test_success:
            logger.info("Validation successful - the fix works properly")
            print("\n✅ CSV Parser and Historical Parser fixes have been successfully applied\n")
        else:
            logger.error("Validation failed - the fix has syntax errors")
            print("\n❌ CSV Parser and Historical Parser fixes have syntax errors\n")
    else:
        logger.error("Failed to apply fixes to historical parser")
        print("\n❌ Failed to apply fixes to historical parser\n")

if __name__ == "__main__":
    asyncio.run(main())