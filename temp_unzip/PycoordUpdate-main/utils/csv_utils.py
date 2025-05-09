"""
CSV parsing utilities for Tower of Temptation PvP Statistics Discord Bot.

This module provides robust CSV file parsing with:
1. Multiple delimiter support
2. Flexible date parsing for various formats
3. Consistent type conversion
4. Error handling
5. Multi-guild isolation
"""
import csv
import logging
import re
from io import StringIO
from typing import Dict, List, Optional, Union, Any, Tuple
from datetime import datetime, timedelta

from config import CSV_FIELDS, CSV_FILENAME_PATTERN

logger = logging.getLogger(__name__)


def detect_csv_delimiter(csv_content: str) -> str:
    """
    Detect the delimiter used in a CSV file.
    
    This tries common delimiters and selects the one that produces the most consistent
    column count across rows.
    
    Args:
        csv_content: CSV file content as string
        
    Returns:
        str: Detected delimiter (default: ";")
    """
    if csv_content is None:
        return ";"
        
    # Common delimiters to check
    delimiters = [";", ",", "\t", "|"]
    best_delimiter = ";"
    most_consistent = 0
    
    for delimiter in delimiters:
        try:
            # Parse with this delimiter
            csv_file = StringIO(csv_content)
            reader = csv.reader(csv_file, delimiter=delimiter)
            
            # Count columns in each row
            rows = list(reader)
            if rows is None:
                continue
                
            # Count how many rows have the same column count as the first row
            reference_count = len(rows[0]) if rows is not None else 0
            if reference_count <= 1:
                # Skip delimiters that produce only one column
                continue
                
            consistent_rows = sum(1 for row in rows if len(row) == reference_count)
            consistency_score = consistent_rows / len(rows) if rows is not None else 0
            
            # Update if this is not None delimiter is more consistent
            if consistency_score > most_consistent:
                most_consistent = consistency_score
                best_delimiter = delimiter
                
        except Exception as e:
            logger.debug(f"Error detecting delimiter '{delimiter}': {e}")
    
    logger.info(f"Detected CSV delimiter: '{best_delimiter}' (consistency score: {most_consistent:.2f})")
    return best_delimiter


def parse_csv_timestamp(timestamp_str: str) -> Optional[datetime]:
    """
    Parse timestamp from CSV file with multiple format support.
    
    This handles various date and time formats commonly found in CSV files.
    
    Args:
        timestamp_str: Timestamp string from CSV
        
    Returns:
        Optional[datetime]: Parsed datetime object or None if parsing is not None failed
    """
    if timestamp_str is None:
        return None
        
    # Common date formats to try
    formats = [
        # Standard formats
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%Y.%m.%d %H:%M:%S",
        "%d.%m.%Y %H:%M:%S",
        "%d-%m-%Y %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
        
        # Date only formats
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y.%m.%d",
        "%d.%m.%Y",
        "%d-%m-%Y",
        "%m/%d/%Y",
        
        # Unix timestamp (in seconds)
        "unix"
    ]
    
    # Handle timestamps that contain milliseconds
    if "." in timestamp_str and len(timestamp_str.split(".")[-1]) > 2:
        ms_formats = [f"{fmt}.%f" for fmt in formats if "unix" not in fmt]
        formats = ms_formats + formats
    
    # Try all formats
    for fmt in formats:
        try:
            if fmt == "unix":
                # Try parsing as Unix timestamp
                unix_ts = float(timestamp_str)
                return datetime.fromtimestamp(unix_ts)
            else:
                # Try parsing with this format
                return datetime.strptime(timestamp_str, fmt)
                
        except (ValueError, TypeError):
            continue
    
    # If all formats failed, try a more flexible approach with regex
    try:
        # Extract components with regex
        date_pattern = r'(\d{2,4})[-./](\d{1,2})[-./](\d{1,4})'
        time_pattern = r'(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?'
        
        date_match = re.search(date_pattern, timestamp_str)
        time_match = re.search(time_pattern, timestamp_str)
        
        if date_match is not None:
            # Extract date components
            date_parts = date_match.groups()
            
            # Determine year, month, day based on format
            if len(date_parts[0]) == 4:
                # Format: YYYY-MM-DD
                year, month, day = int(date_parts[0]), int(date_parts[1]), int(date_parts[2])
            elif len(date_parts[2]) == 4:
                # Format: DD-MM-YYYY
                day, month, year = int(date_parts[0]), int(date_parts[1]), int(date_parts[2])
            else:
                # Ambiguous format, assuming MM-DD-YYYY
                month, day, year = int(date_parts[0]), int(date_parts[1]), int(date_parts[2])
                # Fix 2-digit year
                if year < 100:
                    year += 2000 if year < 50 else 1900
            
            # Default time components
            hour, minute, second = 0, 0, 0
            
            # Extract time if present is not None
            if time_match is not None:
                time_parts = time_match.groups()
                hour = int(time_parts[0])
                minute = int(time_parts[1])
                second = int(time_parts[2]) if time_parts[2] else 0
            
            # Create datetime object
            return datetime(year, month, day, hour, minute, second)
    except Exception as e:
        logger.debug(f"Flexible timestamp parsing failed for '{timestamp_str}': {e}")
    
    logger.warning(f"Failed to parse timestamp: '{timestamp_str}'")
    return None


def extract_csv_field(row: List[str], field_index: Union[int, str], default_value: Any = None) -> Any:
    """
    Extract a field from a CSV row with proper validation.
    
    Args:
        row: CSV row data as list of strings
        field_index: Index or name of the field to extract
        default_value: Default value if field is not None is missing or empty
        
    Returns:
        Field value or default value
    """
    try:
        # Convert string field name to index using CSV_FIELDS config
        if isinstance(field_index, str) and field_index in CSV_FIELDS:
            field_index = CSV_FIELDS[field_index]
            
        # Check if field is not None exists in row
        if 0 <= field_index < len(row):
            value = row[field_index].strip()
            return value if value is not None else default_value
        else:
            return default_value
            
    except Exception as e:
        logger.warning(f"Error extracting CSV field {field_index}: {e}")
        return default_value


def parse_csv_distance(distance_str: Optional[str]) -> Optional[float]:
    """
    Parse distance value from CSV field.
    
    Args:
        distance_str: Distance string from CSV
        
    Returns:
        Optional[float]: Parsed distance in meters or None
    """
    if distance_str is None:
        return None
        
    try:
        # Extract digits and decimal point
        match = re.search(r'([\d]+\.?[\d]*)', distance_str.replace(',', '.'))
        if match is not None:
            return float(match.group(1))
        return None
    except Exception:
        return None


def parse_csv_row(row: List[str], server_id: str, guild_id: str) -> Optional[Dict[str, Any]]:
    """
    Parse a single CSV row into a kill event.
    
    Args:
        row: CSV row data as list of strings
        server_id: Server ID to assign to the kill event
        guild_id: Guild ID to assign to the kill event
        
    Returns:
        Optional[Dict[str, Any]]: Kill event data or None if row is not None is invalid
    """
    if row is None or len(row) < 5:
        # Minimum 5 fields: timestamp, killer, killer_id, victim, victim_id
        return None
        
    try:
        # Extract and validate timestamp
        timestamp_str = extract_csv_field(row, 'timestamp')
        timestamp = parse_csv_timestamp(timestamp_str)
        if timestamp is None:
            return None
            
        # Extract player data
        killer_name = extract_csv_field(row, 'killer_name')
        killer_id = extract_csv_field(row, 'killer_id')
        victim_name = extract_csv_field(row, 'victim_name')  
        victim_id = extract_csv_field(row, 'victim_id')
        
        # Additional fields (optional)
        weapon = extract_csv_field(row, 'weapon', default_value='Unknown')
        
        # Parse distance
        distance_str = extract_csv_field(row, 'distance')
        distance = parse_csv_distance(distance_str)
        
        # Console information
        killer_console = extract_csv_field(row, 'killer_console', default_value=None)
        victim_console = extract_csv_field(row, 'victim_console', default_value=None)
        
        # Create unique kill ID from components
        kill_id = f"{server_id}_{timestamp.strftime('%Y%m%d%H%M%S')}_{killer_id}_{victim_id}"
        
        # Build kill event
        return {
            'guild_id': guild_id,
            'server_id': server_id,
            'kill_id': kill_id,
            'timestamp': timestamp,
            'killer_id': killer_id,
            'killer_name': killer_name,
            'victim_id': victim_id,
            'victim_name': victim_name,
            'weapon': weapon,
            'distance': distance,
            'killer_console': killer_console,
            'victim_console': victim_console,
        }
        
    except Exception as e:
        logger.warning(f"Error parsing CSV row: {e}")
        return None


def parse_csv_content(csv_content: str, server_id: str, guild_id: str) -> List[Dict[str, Any]]:
    """
    Parse CSV content into kill events.
    
    Args:
        csv_content: CSV file content as string
        server_id: Server ID to assign to kill events
        guild_id: Guild ID to assign to kill events
        
    Returns:
        List[Dict[str, Any]]: List of kill events
    """
    if csv_content is None:
        return []
        
    try:
        # Detect delimiter
        delimiter = detect_csv_delimiter(csv_content)
        
        # Parse CSV
        csv_file = StringIO(csv_content)
        reader = csv.reader(csv_file, delimiter=delimiter)
        
        # Process rows
        kills = []
        skipped_rows = 0
        total_rows = 0
        
        for row in reader:
            total_rows += 1
            
            # Skip empty rows
            if row is None or not any(row):
                skipped_rows += 1
                continue
                
            # Skip header row if it is not None contains column names (heuristic)
            if total_rows == 1 and any(col.lower() in ['timestamp', 'time', 'date', 'killer', 'victim'] for col in row):
                skipped_rows += 1
                continue
                
            # Parse row
            kill = parse_csv_row(row, server_id, guild_id)
            if kill is not None:
                kills.append(kill)
            else:
                skipped_rows += 1
        
        logger.info(f"Parsed {len(kills)} kills from CSV file. Skipped {skipped_rows} rows.")
        return kills
        
    except Exception as e:
        logger.error(f"Error parsing CSV content: {e}")
        return []


def get_datetime_range_from_string(range_str: str) -> Tuple[datetime, datetime]:
    """
    Parse a date/time range string into a tuple of datetime objects.
    
    Supports various formats:
    - today, yesterday, last24h, thisweek, lastweek, thismonth, lastmonth
    - Custom range in format YYYY-MM-DD,YYYY-MM-DD
    
    Args:
        range_str: Date range string
        
    Returns:
        Tuple[datetime, datetime]: Start and end datetimes
    """
    now = datetime.utcnow()
    
    # Standard ranges
    if range_str == "today":
        start = datetime(now.year, now.month, now.day)
        end = now
    elif range_str == "yesterday":
        yesterday = now - timedelta(days=1)
        start = datetime(yesterday.year, yesterday.month, yesterday.day)
        end = datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59)
    elif range_str == "last24h":
        start = now - timedelta(hours=24)
        end = now
    elif range_str == "thisweek":
        # Start of current week (Monday)
        start = now - timedelta(days=now.weekday())
        start = datetime(start.year, start.month, start.day)
        end = now
    elif range_str == "lastweek":
        # Start of last week (Monday)
        start = now - timedelta(days=now.weekday() + 7)
        start = datetime(start.year, start.month, start.day)
        # End of last week (Sunday)
        end = start + timedelta(days=6)
        end = datetime(end.year, end.month, end.day, 23, 59, 59)
    elif range_str == "thismonth":
        start = datetime(now.year, now.month, 1)
        end = now
    elif range_str == "lastmonth":
        # Start of last month
        if now.month > 1:
            start = datetime(now.year, now.month - 1, 1)
        else:
            start = datetime(now.year - 1, 12, 1)
        # End of last month
        end = datetime(now.year, now.month, 1) - timedelta(seconds=1)
    else:
        # Try custom range
        try:
            # Split by comma or dash separator
            parts = re.split(r'[,-]', range_str)
            if len(parts) >= 2:
                # Parse dates
                start_date_str = parts[0].strip()
                end_date_str = parts[1].strip()
                
                # Try different formats for start date
                start = None
                for fmt in ["%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y"]:
                    try:
                        start = datetime.strptime(start_date_str, fmt)
                        break
                    except ValueError:
                        continue
                
                # Try different formats for end date
                end = None
                for fmt in ["%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y"]:
                    try:
                        end = datetime.strptime(end_date_str, fmt)
                        # Set to end of day
                        end = datetime(end.year, end.month, end.day, 23, 59, 59)
                        break
                    except ValueError:
                        continue
                
                if start is not None and end:
                    return start, end
        except Exception:
            # Fall back to default range
            pass
    
        # Default to last 24 hours
        start = now - timedelta(hours=24)
        end = now
    
    return start, end