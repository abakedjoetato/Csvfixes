"""
Final implementation of the CSV timestamp parsing fix.

This script directly applies the timestamp parsing fix to ensure that
CSV files with the format YYYY.MM.DD-HH.MM.SS.csv are properly parsed.
"""

import logging
import sys
import os
from datetime import datetime, timedelta
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("timestamp_fix.log")
    ]
)

logger = logging.getLogger(__name__)

def backup_file(file_path):
    """Make a backup of a file before modifying it"""
    import shutil
    import time
    
    # Create backups directory if it doesn't exist
    backups_dir = os.path.join(os.path.dirname(file_path), "backups")
    os.makedirs(backups_dir, exist_ok=True)
    
    # Create backup filename with timestamp
    timestamp = time.strftime("%Y%m%d%H%M%S")
    file_name = os.path.basename(file_path)
    backup_path = os.path.join(backups_dir, f"{file_name}.{timestamp}.bak")
    
    # Copy the file
    shutil.copy2(file_path, backup_path)
    logger.info(f"Created backup of {file_path} at {backup_path}")
    
    return backup_path

def fix_csv_parser():
    """Apply the timestamp parsing fix to the CSV parser"""
    csv_parser_path = "utils/csv_parser.py"
    
    if not os.path.exists(csv_parser_path):
        logger.error(f"CSV parser file not found at {csv_parser_path}")
        return False
    
    # Create backup
    backup_file(csv_parser_path)
    
    # Read the file
    with open(csv_parser_path, 'r') as file:
        content = file.read()
    
    # Check if the fix already seems to be implemented
    if "TIMESTAMP_FORMATS = [" in content and "%Y.%m.%d-%H.%M.%S" in content:
        logger.info("Fix already seems to be implemented in CSV parser")
        return True
    
    # Apply the fix - look for the timestamp format definition and enhance it
    timestamp_formats_pattern = r"(\s+)# Define timestamp format options\s+(.+?)(\s+)# Try each format until one works"
    
    replacement = r"""\1# Define timestamp format options
\1TIMESTAMP_FORMATS = [
\1    "%Y.%m.%d-%H.%M.%S",  # Primary format: 2025.05.09-11.31.06 
\1    "%Y.%m.%d-%H:%M:%S",  # Alternative format with colons: 2025.05.09-11:31:06
\1    "%Y-%m-%d-%H.%M.%S",  # Alternative format with dashes: 2025-05-09-11.31.06
\1    "%Y-%m-%d %H:%M:%S",  # ISO-like format: 2025-05-09 11:31:06
\1    "%Y.%m.%d %H:%M:%S",  # Mixed format: 2025.05.09 11:31:06
\1]
\1last_format_used = None  # Track which format was used last\3# Try each format until one works"""
    
    # Perform the replacement
    modified_content = re.sub(timestamp_formats_pattern, replacement, content, flags=re.DOTALL)
    
    # Check if anything changed
    if modified_content == content:
        logger.warning("Could not find timestamp format section to modify")
        return False
    
    # Now update the parse_timestamp method to use the formats list
    parse_timestamp_pattern = r"(\s+)def parse_timestamp\(self, timestamp_str\):(.*?)(\s+)# Try parsing with different formats(.*?)(\s+)return None"
    
    parse_timestamp_replacement = r"""\1def parse_timestamp(self, timestamp_str):
\1    """Parse a timestamp string into a datetime object
\1    
\1    Args:
\1        timestamp_str: Timestamp string to parse
\1        
\1    Returns:
\1        datetime: Parsed datetime object or None if parsing failed
\1    """
\1    if not timestamp_str:
\1        return None
\1        
\1    timestamp_str = timestamp_str.strip()
\1    
\1    # Try each defined format
\1    for format_str in self.TIMESTAMP_FORMATS:
\1        try:
\1            dt = datetime.strptime(timestamp_str, format_str)
\1            self.last_format_used = format_str  # Track which format worked
\1            return dt
\1        except ValueError:
\1            continue
\1            
\1    # If all formats failed, log the issue and return None
\1    logger.warning(f"Could not parse timestamp: {timestamp_str}")
\1    return None"""
    
    # Apply the parse_timestamp method fix
    modified_content = re.sub(parse_timestamp_pattern, parse_timestamp_replacement, modified_content, flags=re.DOTALL)
    
    # Write the modified content back to the file
    with open(csv_parser_path, 'w') as file:
        file.write(modified_content)
    
    logger.info(f"Successfully applied the timestamp parsing fix to {csv_parser_path}")
    
    return True

def fix_parser_utils():
    """Apply necessary fixes to parser_utils.py"""
    parser_utils_path = "utils/parser_utils.py"
    
    if not os.path.exists(parser_utils_path):
        logger.error(f"Parser utils file not found at {parser_utils_path}")
        return False
    
    # Create backup
    backup_file(parser_utils_path)
    
    # Read the file
    with open(parser_utils_path, 'r') as file:
        content = file.read()
    
    # Check if the correct timestamp comparison function exists
    if "def is_timestamp_newer_than(timestamp, cutoff_date):" in content:
        logger.info("Timestamp comparison function already exists in parser_utils.py")
        return True
    
    # Add the timestamp comparison function if needed
    timestamp_compare_function = """
def is_timestamp_newer_than(timestamp, cutoff_date):
    """Check if a timestamp is newer than a cutoff date
    
    Args:
        timestamp: datetime object to check
        cutoff_date: datetime object to compare against
        
    Returns:
        bool: True if timestamp is newer than cutoff_date, False otherwise
    """
    if not timestamp or not cutoff_date:
        return False
        
    return timestamp > cutoff_date
"""
    
    # Check if the function needs to be added
    if "is_timestamp_newer_than" not in content:
        # Add it to the end of the file
        with open(parser_utils_path, 'a') as file:
            file.write(timestamp_compare_function)
        
        logger.info(f"Added timestamp comparison function to {parser_utils_path}")
    
    return True

def main():
    """Main entry point"""
    logger.info("Starting final CSV timestamp parsing fix application")
    
    # Apply the fixes
    csv_parser_fixed = fix_csv_parser()
    parser_utils_fixed = fix_parser_utils()
    
    if csv_parser_fixed and parser_utils_fixed:
        logger.info("Successfully applied all timestamp parsing fixes")
        return True
    else:
        logger.error("Failed to apply some fixes")
        return False

if __name__ == "__main__":
    sys.exit(0 if main() else 1)