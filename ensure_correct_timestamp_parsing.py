"""
Timestamp Parsing Fix for CSV Processor

This script specifically fixes the timestamp parsing issues in the CSV processor.
It ensures that the YYYY.MM.DD-HH.MM.SS format is correctly parsed.
"""

import os
import re
from datetime import datetime
import logging
import sys
import asyncio

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('timestamp_fix.log')
    ]
)

logger = logging.getLogger(__name__)

async def fix_csv_parser_timestamp_format():
    """Fix the timestamp format in utils/csv_parser.py"""
    logger.info("Fixing timestamp format in CSV parser...")
    
    csv_parser_path = "utils/csv_parser.py"
    if not os.path.exists(csv_parser_path):
        logger.error(f"CSV parser file not found: {csv_parser_path}")
        return False
    
    try:
        with open(csv_parser_path, 'r') as f:
            content = f.read()
        
        # Check if the primary datetime format is already correct
        if 'datetime_format": "%Y.%m.%d-%H.%M.%S"' in content:
            logger.info("CSV parser already has the correct datetime format")
        else:
            # Update the datetime format
            content = re.sub(
                r'datetime_format": "[^"]*"',
                'datetime_format": "%Y.%m.%d-%H.%M.%S"',
                content
            )
            logger.info("Updated datetime format in CSV parser")
            
            # Save updated content
            with open(csv_parser_path, 'w') as f:
                f.write(content)
        
        return True
    except Exception as e:
        logger.error(f"Error fixing CSV parser: {str(e)}")
        return False

async def fix_csv_processor_date_extraction():
    """Fix date extraction in cogs/csv_processor.py"""
    logger.info("Fixing date extraction in CSV processor...")
    
    csv_processor_path = "cogs/csv_processor.py"
    if not os.path.exists(csv_processor_path):
        logger.error(f"CSV processor file not found: {csv_processor_path}")
        return False
    
    try:
        with open(csv_processor_path, 'r') as f:
            content = f.read()
        
        # 1. Ensure the filename regex pattern correctly extracts YYYY.MM.DD-HH.MM.SS
        filename_pattern = r'(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2})'
        if filename_pattern not in content:
            # Find the line with date extraction pattern
            old_pattern_match = re.search(r'date_match = re\.search\(r[\'"]([^\'"]+)[\'"]', content)
            if old_pattern_match:
                old_pattern = old_pattern_match.group(1)
                content = content.replace(
                    f'date_match = re.search(r\'{old_pattern}\'',
                    f'date_match = re.search(r\'{filename_pattern}\''
                )
                logger.info(f"Updated date extraction pattern from '{old_pattern}' to '{filename_pattern}'")
            else:
                logger.warning("Could not find date extraction pattern to update")
        
        # 2. Ensure the strptime format is correct
        strptime_format = '"%Y.%m.%d-%H.%M.%S"'
        if 'datetime.strptime(file_date_str, "%Y.%m.%d-%H.%M.%S")' not in content:
            # Find the line with strptime for file dates
            strptime_match = re.search(r'file_date = datetime\.strptime\(file_date_str, ([^\)]+)\)', content)
            if strptime_match:
                old_format = strptime_match.group(1)
                content = content.replace(
                    f'file_date = datetime.strptime(file_date_str, {old_format})',
                    f'file_date = datetime.strptime(file_date_str, {strptime_format})'
                )
                logger.info(f"Updated datetime.strptime format from {old_format} to {strptime_format}")
            else:
                logger.warning("Could not find datetime.strptime format to update")
        
        # 3. Add enhanced date parsing with fallbacks
        enhance_pattern = """
                                            # Try all possible datetime formats to ensure we don't miss valid dates
                                            parsed = False
                                            for date_format in [
                                                "%Y.%m.%d-%H.%M.%S",  # Primary format: 2025.05.03-00.00.00
                                                "%Y-%m-%d-%H.%M.%S",  # Alternative dash format
                                                "%Y.%m.%d-%H:%M:%S",  # Mixed format with colons
                                                "%Y-%m-%d %H:%M:%S",  # Standard ISO-like format 
                                                "%Y.%m.%d %H:%M:%S",  # Dots for date with standard time
                                            ]:
                                                try:
                                                    logger.warning(f"ENHANCED PARSING: Trying format {date_format} for {file_date_str}")
                                                    file_date = datetime.strptime(file_date_str, date_format)
                                                    last_time_date = datetime.strptime(last_time_str, "%Y.%m.%d-%H.%M.%S")
                                                    logger.warning(f"ENHANCED PARSING: Successfully parsed {file_date_str} as {file_date}")
                                                    parsed = True
                                                    break
                                                except ValueError:
                                                    continue
                                                    
                                            if not parsed:
                                                logger.warning(f"ENHANCED PARSING: Could not parse {file_date_str} with any format, skipping file")
                                                skipped_files.append(f)
                                                continue
        """
        
        # Find where to insert the enhanced parsing
        if "ENHANCED PARSING" not in content:
            # Find the section with date comparison
            date_comparison = re.search(r'file_date = datetime\.strptime\(file_date_str, [^\)]+\)(.*?)if file_date > last_time:', content, re.DOTALL)
            if date_comparison:
                old_block = date_comparison.group(0)
                
                # Check if there's error handling
                if "except ValueError:" in old_block:
                    # Replace the error handling with our enhanced parsing
                    enhanced_block = re.sub(
                        r'file_date = datetime\.strptime\(file_date_str, [^\)]+\)(.*?)if file_date > last_time:',
                        r'try:\n                                            file_date = datetime.strptime(file_date_str, "%Y.%m.%d-%H.%M.%S")\n                                        except ValueError:' + enhance_pattern + r'\n                                            if file_date > last_time:',
                        old_block,
                        flags=re.DOTALL
                    )
                    content = content.replace(old_block, enhanced_block)
                    logger.info("Added enhanced date parsing with fallbacks")
                else:
                    # Add error handling with our enhanced parsing
                    enhanced_block = old_block.replace(
                        'file_date = datetime.strptime(file_date_str, "%Y.%m.%d-%H.%M.%S")',
                        'try:\n                                            file_date = datetime.strptime(file_date_str, "%Y.%m.%d-%H.%M.%S")\n                                        except ValueError:' + enhance_pattern
                    )
                    content = content.replace(old_block, enhanced_block)
                    logger.info("Added enhanced date parsing with fallbacks")
            else:
                logger.warning("Could not find date comparison section to enhance")
        
        # 4. Add logging for date comparison
        if "DEBUG DATE COMPARISON" not in content:
            # Find the date comparison section
            date_comparison = re.search(r'if file_date > last_time:(.*?)new_files\.append\(f\)', content, re.DOTALL)
            if date_comparison:
                old_block = date_comparison.group(0)
                enhanced_block = old_block.replace(
                    'if file_date > last_time:',
                    'logger.warning(f"DEBUG DATE COMPARISON: File date {file_date} > Last time {last_time}? {file_date > last_time}")\n                                    if file_date > last_time:'
                )
                content = content.replace(old_block, enhanced_block)
                logger.info("Added detailed logging for date comparison")
            else:
                logger.warning("Could not find date comparison section to enhance with logging")
        
        # Save updated content
        with open(csv_processor_path, 'w') as f:
            f.write(content)
        
        logger.info("Successfully fixed date extraction in CSV processor")
        return True
    except Exception as e:
        logger.error(f"Error fixing CSV processor: {str(e)}")
        return False

async def main():
    """Main function to run all fixes"""
    logger.info("Starting timestamp parsing fixes...")
    
    # Fix the CSV parser
    parser_fixed = await fix_csv_parser_timestamp_format()
    
    # Fix the CSV processor
    processor_fixed = await fix_csv_processor_date_extraction()
    
    if parser_fixed and processor_fixed:
        logger.info("Successfully applied all timestamp parsing fixes")
    else:
        logger.error("Failed to apply some timestamp parsing fixes")
    
    logger.info("Timestamp parsing fixes completed")

if __name__ == "__main__":
    asyncio.run(main())