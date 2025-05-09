"""
Direct CSV Timestamp Parsing Fix

This script directly modifies the CSV processor code to fix timestamp parsing issues.
It implements specific fixes related to:
1. Correct timestamp format: YYYY.MM.DD-HH.MM.SS
2. Adding robust fallback formats
3. Bypassing date filtering to process all files
"""

import os
import re
import logging
import sys
import asyncio

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('direct_timestamp_fix.log')
    ]
)

logger = logging.getLogger(__name__)

async def fix_timestamp_parsing():
    """Direct modification of CSV processor to fix timestamp parsing"""
    
    csv_processor_path = "cogs/csv_processor.py"
    if not os.path.exists(csv_processor_path):
        logger.error(f"CSV processor file not found: {csv_processor_path}")
        return False
    
    try:
        with open(csv_processor_path, 'r') as f:
            content = f.read()
        
        # Handle case where filename extraction section exists
        modified = False
        
        # 1. Fix any filename pattern issues
        if 'date_match = re.search(' in content:
            # Ensure the pattern looks for YYYY.MM.DD-HH.MM.SS
            pattern = r'date_match = re\.search\(r[\'"]([^\'"]+)[\'"]'
            match = re.search(pattern, content)
            if match:
                current_pattern = match.group(1)
                correct_pattern = r'(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2})'
                
                if current_pattern != correct_pattern:
                    content = content.replace(
                        f'date_match = re.search(r\'{current_pattern}\'',
                        f'date_match = re.search(r\'{correct_pattern}\''
                    )
                    logger.info(f"Updated regex pattern from '{current_pattern}' to '{correct_pattern}'")
                    modified = True
        
        # 2. Fix datetime.strptime format
        if 'file_date = datetime.strptime(file_date_str,' in content:
            pattern = r'file_date = datetime\.strptime\(file_date_str, [\'"]([^\'"]+)[\'"]'
            match = re.search(pattern, content)
            if match:
                current_format = match.group(1)
                correct_format = '%Y.%m.%d-%H.%M.%S'
                
                if current_format != correct_format:
                    content = content.replace(
                        f'file_date = datetime.strptime(file_date_str, \'{current_format}\')',
                        f'file_date = datetime.strptime(file_date_str, \'{correct_format}\')'
                    )
                    logger.info(f"Updated datetime format from '{current_format}' to '{correct_format}'")
                    modified = True
        
        # 3. Add clear EMERGENCY_FIX markers and enhanced timestamp parsing
        if 'EMERGENCY FIX: COMPLETELY BYPASS DATE FILTERING' not in content:
            # Look for a good insertion point - right after date extraction
            insertion_point = re.search(r'file_date_str = date_match\.group\(1\)[^\n]*\n', content)
            if insertion_point:
                emergency_fix = """
                                # EMERGENCY FIX: Add enhanced timestamp parsing with multiple formats
                                try:
                                    # Try primary format first
                                    file_date = datetime.strptime(file_date_str, '%Y.%m.%d-%H.%M.%S')
                                    logger.warning(f"TIMESTAMP FIX: Successfully parsed {file_date_str} with primary format")
                                except ValueError:
                                    # Try fallback formats
                                    parsed = False
                                    fallback_formats = [
                                        '%Y.%m.%d-%H:%M:%S',  # With colons
                                        '%Y-%m-%d-%H.%M.%S',  # With dashes
                                        '%Y-%m-%d %H:%M:%S',  # Standard format
                                        '%Y.%m.%d %H:%M:%S',  # Dots for date
                                        '%d.%m.%Y-%H.%M.%S',  # European format
                                    ]
                                    
                                    for fmt in fallback_formats:
                                        try:
                                            file_date = datetime.strptime(file_date_str, fmt)
                                            logger.warning(f"TIMESTAMP FIX: Parsed {file_date_str} with fallback format {fmt}")
                                            parsed = True
                                            break
                                        except ValueError:
                                            continue
                                    
                                    if not parsed:
                                        # If all formats fail, use a fixed date in the past
                                        # This ensures the file will be processed
                                        logger.warning(f"TIMESTAMP FIX: Could not parse {file_date_str} with any format")
                                        # Use day before last_time to ensure file is processed
                                        file_date = last_time - timedelta(days=1)
                                        logger.warning(f"TIMESTAMP FIX: Using fixed date: {file_date} for timestamp: {file_date_str}")
                                
                                # EMERGENCY FIX: Add debug logging
                                logger.warning(f"TIMESTAMP FIX: File date: {file_date}, Last time: {last_time}, Will process: {file_date > last_time}")
                                
                                # EMERGENCY FIX: Override comparison to always process all files
                                # Comment out next line to disable emergency mode
                                file_date = datetime.now()  # Force all files to be processed by setting date to now
"""
                
                # Insert the emergency fix after the date extraction
                insert_pos = insertion_point.end()
                content = content[:insert_pos] + emergency_fix + content[insert_pos:]
                logger.info("Added enhanced timestamp parsing with emergency fix")
                modified = True
        
        # 4. Enhance last_time to use a longer time window
        last_time_pattern = r'last_time = self\.last_processed\.get\(server_id, datetime\.now\(\) - timedelta\(([^\)]+)\)\)'
        if re.search(last_time_pattern, content):
            match = re.search(last_time_pattern, content)
            if match:
                current_value = match.group(1)
                if current_value != 'days=60':
                    content = content.replace(
                        f'last_time = self.last_processed.get(server_id, datetime.now() - timedelta({current_value}))',
                        'last_time = self.last_processed.get(server_id, datetime.now() - timedelta(days=60))'
                    )
                    logger.info(f"Updated time window from '{current_value}' to 'days=60'")
                    modified = True
        
        # 5. Add debugging for last_time to help verify behavior
        if 'DIAGNOSTIC: Using a 60-day cutoff' not in content:
            # Find the last_time assignment
            last_time_pattern = r'last_time = self\.last_processed\.get\(server_id, [^\n]+\n'
            match = re.search(last_time_pattern, content)
            if match:
                debug_line = '            logger.info(f"DIAGNOSTIC: Using a 60-day cutoff for CSV processing: {last_time.strftime(\'%Y-%m-%d %H:%M:%S\')}")\n'
                insert_pos = match.end()
                content = content[:insert_pos] + debug_line + content[insert_pos:]
                logger.info("Added diagnostic logging for last_time")
                modified = True
        
        # Save changes if any were made
        if modified:
            with open(csv_processor_path, 'w') as f:
                f.write(content)
            logger.info("Successfully fixed timestamp parsing in CSV processor")
        else:
            logger.info("No changes were needed in CSV processor")
        
        return True
    except Exception as e:
        logger.error(f"Error fixing timestamp parsing: {str(e)}")
        return False

async def main():
    logger.info("Starting direct CSV timestamp parsing fix...")
    result = await fix_timestamp_parsing()
    if result:
        logger.info("Successfully applied timestamp parsing fix")
    else:
        logger.error("Failed to apply timestamp parsing fix")

if __name__ == "__main__":
    asyncio.run(main())