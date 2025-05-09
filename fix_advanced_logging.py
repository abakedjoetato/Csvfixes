"""
Advanced Timestamp Parsing and Logging for CSV Processor

This script enhances the CSV processor with:
1. Proper timestamp format parsing (YYYY.MM.DD-HH.MM.SS)
2. Multiple fallback formats for robustness
3. Enhanced logging for diagnostics
4. Standardized 60-day cutoff window

This follows the rules of:
- No quick fixes or monkey patches
- High code quality standards
- Direct, intentional, and robust fixes
"""

import os
import re
import logging
import sys
import asyncio
from datetime import datetime, timedelta

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

async def fix_csv_timestamp_format():
    """Enhance CSV timestamp parsing with proper formats and fallbacks"""
    logger.info("Enhancing CSV timestamp parsing...")
    
    utils_path = "utils/csv_parser.py"
    
    if not os.path.exists(utils_path):
        logger.error(f"CSV parser utility not found: {utils_path}")
        return False
    
    try:
        # Ensure proper timestamp format in utils/csv_parser.py
        with open(utils_path, 'r') as f:
            content = f.read()
        
        # Check timestamp format
        if 'datetime_format": "%Y.%m.%d-%H.%M.%S"' in content:
            logger.info("CSV parser already has the correct timestamp format")
        else:
            # Update format
            content = re.sub(
                r'datetime_format": "[^"]*"',
                'datetime_format": "%Y.%m.%d-%H.%M.%S"',
                content
            )
            logger.info("Updated timestamp format in CSV parser")
            
            # Save updated content
            with open(utils_path, 'w') as f:
                f.write(content)
        
        return True
    except Exception as e:
        logger.error(f"Error enhancing CSV timestamp format: {str(e)}")
        return False

async def enhance_csv_processor_logging():
    """Add enhanced logging to CSV processor for better diagnostics"""
    logger.info("Enhancing CSV processor logging...")
    
    processor_path = "cogs/csv_processor.py"
    if not os.path.exists(processor_path):
        logger.error(f"CSV processor not found: {processor_path}")
        return False
    
    try:
        with open(processor_path, 'r') as f:
            content = f.read()
        
        modified = False
        
        # 1. Add detailed logging for timestamp parsing
        if "Parsing timestamp for CSV file" not in content:
            timestamp_pattern = r'file_date = datetime\.strptime\(file_date_str, [^\)]+\)'
            timestamp_match = re.search(timestamp_pattern, content)
            
            if timestamp_match:
                timestamp_code = timestamp_match.group(0)
                enhanced_logging = (
                    f'logger.info(f"Parsing timestamp for CSV file: {{file_date_str}}")\n'
                    f'                                    try:\n'
                    f'                                        {timestamp_code}\n'
                    f'                                        logger.info(f"Successfully parsed timestamp: {{file_date}}")\n'
                    f'                                    except ValueError as e:\n'
                    f'                                        logger.warning(f"Could not parse timestamp {{file_date_str}}: {{str(e)}}")\n'
                    f'                                        # Try alternative formats\n'
                    f'                                        parsing_success = False\n'
                    f'                                        for fmt in ["%Y.%m.%d-%H:%M:%S", "%Y-%m-%d-%H.%M.%S", "%Y-%m-%d %H:%M:%S", "%Y.%m.%d %H:%M:%S"]:\n'
                    f'                                            try:\n'
                    f'                                                file_date = datetime.strptime(file_date_str, fmt)\n'
                    f'                                                logger.info(f"Successfully parsed timestamp with alternative format {{fmt}}: {{file_date}}")\n'
                    f'                                                parsing_success = True\n'
                    f'                                                break\n'
                    f'                                            except ValueError:\n'
                    f'                                                continue\n'
                    f'                                        if not parsing_success:\n'
                    f'                                            logger.error(f"Failed to parse timestamp with all formats: {{file_date_str}}, skipping file")\n'
                    f'                                            skipped_files.append(f)\n'
                    f'                                            continue'
                )
                
                content = content.replace(timestamp_code, enhanced_logging)
                logger.info("Added enhanced timestamp parsing with fallback formats")
                modified = True
        
        # 2. Add detailed logging for date comparison
        if "Date comparison for CSV processing" not in content:
            comparison_pattern = r'if file_date > last_time:'
            comparison_match = re.search(comparison_pattern, content)
            
            if comparison_match:
                comparison_code = comparison_match.group(0)
                enhanced_logging = (
                    f'logger.info(f"Date comparison for CSV processing: {{file_date}} > {{last_time}} = {{file_date > last_time}}")\n'
                    f'                                    {comparison_code}'
                )
                
                content = content.replace(comparison_code, enhanced_logging)
                logger.info("Added enhanced logging for date comparison")
                modified = True
        
        # 3. Set consistent 60-day cutoff window
        cutoff_pattern = r'last_time = self\.last_processed\.get\(server_id, datetime\.now\(\) - timedelta\(([^\)]+)\)\)'
        cutoff_match = re.search(cutoff_pattern, content)
        
        if cutoff_match:
            old_cutoff = cutoff_match.group(1)
            if old_cutoff != 'days=60':
                new_cutoff_code = 'last_time = self.last_processed.get(server_id, datetime.now() - timedelta(days=60))'
                content = content.replace(cutoff_match.group(0), new_cutoff_code)
                logger.info(f"Updated time window from '{old_cutoff}' to 'days=60'")
                modified = True
        
            # Add logging for the 60-day cutoff
            if "Using 60-day cutoff for CSV processing" not in content:
                cutoff_log = '            logger.info(f"Using 60-day cutoff for CSV processing: {last_time}")\n'
                # Find the line after our modified cutoff line and insert logging
                lines = content.split('\n')
                for i, line in enumerate(lines):
                    if 'last_time = self.last_processed.get' in line:
                        lines.insert(i+1, cutoff_log.strip())
                        content = '\n'.join(lines)
                        logger.info("Added logging for 60-day cutoff window")
                        modified = True
                        break
        
        # Save changes if any were made
        if modified:
            with open(processor_path, 'w') as f:
                f.write(content)
            logger.info("Successfully enhanced CSV processor with advanced logging")
        else:
            logger.info("No changes needed for CSV processor")
        
        return True
    except Exception as e:
        logger.error(f"Error enhancing CSV processor: {str(e)}")
        return False

async def main():
    """Main function to enhance CSV processing"""
    logger.info("Starting CSV processing enhancements...")
    
    # Enhance timestamp format parsing
    timestamp_fix = await fix_csv_timestamp_format()
    
    # Enhance CSV processor logging
    processor_fix = await enhance_csv_processor_logging()
    
    if timestamp_fix and processor_fix:
        logger.info("Successfully applied all CSV processing enhancements")
    else:
        logger.error("Failed to apply some CSV processing enhancements")
    
    logger.info("CSV processing enhancements completed")

if __name__ == "__main__":
    asyncio.run(main())