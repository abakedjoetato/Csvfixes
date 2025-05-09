"""
CSV Parser for the Tower of Temptation PvP Statistics Discord Bot.

This module provides:
1. CSV file parsing with robust error handling
2. Streaming event extraction for large files
3. Resilient log processing with format detection
4. Fault-tolerant statistics aggregation
5. Cross-platform statistics for post-April format
"""
import csv
import io
import re
import logging
import traceback
import os # Added import for os.path.join
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Set, Tuple, BinaryIO, TextIO, Iterator, Generator

logger = logging.getLogger(__name__)

class CSVParser:
    """Enhanced CSV file parser for game log files with robust error handling"""

    # Define standard log formats with fallbacks for format variations
    LOG_FORMATS = {
        "deadside": {
            "separator": ";",
            "columns": ["timestamp", "killer_name", "killer_id", "victim_name", "victim_id", "weapon", "distance", "platform"],
            "datetime_format": "%Y.%m.%d-%H.%M.%S",  # Format matching 2025.03.27-10.42.18
            "datetime_column": "timestamp",
            "fallback_formats": [
                # Pre-April format (7-field CSV)
                {
                    "separator": ";",
                    "columns": ["timestamp", "killer_name", "killer_id", "victim_name", "victim_id", "weapon", "distance"]
                },
                # Post-April format (9-field CSV with console information)
                {
                    "separator": ";",
                    "columns": ["timestamp", "killer_name", "killer_id", "victim_name", "victim_id", "weapon", "distance", "killer_console", "victim_console"]
                },
                # Alternative separators and columns
                {
                    "separator": ",",
                    "columns": ["timestamp", "killer_name", "killer_id", "victim_name", "victim_id", "weapon", "distance", "platform"]
                },
                {
                    "separator": ";",
                    "columns": ["date", "killer_name", "killer_id", "victim_name", "victim_id", "weapon", "distance", "platform"],
                    "datetime_column": "date"
                },
                # Alternative datetime formats
                {
                    "separator": ";",
                    "columns": ["timestamp", "killer_name", "killer_id", "victim_name", "victim_id", "weapon", "distance", "platform"],
                    "datetime_format": "%Y-%m-%d %H:%M:%S"
                }
            ],
            "required_columns": ["killer_name", "victim_name", "weapon"]  # Absolute minimum required
        },
        "custom": {
            "separator": ",",
            "columns": ["timestamp", "event_type", "player1_name", "player1_id", "player2_name", "player2_id", "details", "location"],
            "datetime_format": "%Y-%m-%d %H:%M:%S",
            "datetime_column": "timestamp"
        }
    }

    def __init__(self, format_name: str = "deadside", hostname: str = None, server_id: str = None):
        """Initialize CSV parser with specified format and server info

        Args:
            format_name: Log format name (default: "deadside")
            hostname: Server hostname
            server_id: Original server ID (not UUID)
        """
        self.format_name = format_name
        if hostname and server_id:
            # Use standardized CSV path structure
            clean_hostname = hostname.split(':')[0] if hostname else "server"
            self.base_path = os.path.join("/", f"{clean_hostname}_{server_id}", "actual1", "deathlogs")
        else:
            self.base_path = None

        # Get format configuration
        if format_name in self.LOG_FORMATS:
            self.format_config = self.LOG_FORMATS[format_name]
        else:
            # Default to deadside format
            logger.warning(f"Unknown log format: {format_name}, using deadside format")
            self.format_name = "deadside"
            self.format_config = self.LOG_FORMATS["deadside"]

        # Extract configuration
        self.separator = self.format_config["separator"]
        self.columns = self.format_config["columns"]
        self.datetime_format = self.format_config["datetime_format"]
        self.datetime_column = self.format_config["datetime_column"]

        # Initialize format detection state
        self.detected_format_info = {"april_update": False}  # Default to pre-April format

        # Initialize caches
        self._event_cache = {}
        self._player_stats_cache = {}
        
        # Track last processed line for each file
        self._last_processed_line_count = {}  # {file_path: line_count}

    def clear_cache(self):
        """Clear all parser caches"""
        self._event_cache = {}
        self._player_stats_cache = {}
        self._last_processed_line_count = {}  # Reset line tracking
        logger.info("CSV parser cache cleared")

    def parse_csv_data(self, data: Union[str, bytes]) -> List[Dict[str, Any]]:
        """Parse CSV data and return list of events

        Args:
            data: CSV data string or bytes

        Returns:
            List[Dict]: List of parsed event dictionaries
        """
        # Convert bytes to string if needed
        if isinstance(data, bytes):
            data = data.decode("utf-8", errors="replace")
            
        # Log a small sample of the data for debugging
        sample_text = data[:200] + "..." if len(data) > 200 else data
        logger.info(f"CSV data sample (first 200 chars): {sample_text}")
        
        # Quick check to see if this looks like CSV data
        sample_chunk = data[:500] if len(data) > 500 else data
        if ";" in sample_chunk or "," in sample_chunk:
            logger.info(f"Data appears to be CSV format (found delimiters)")
        else:
            logger.warning(f"Data might not be CSV format (no common delimiters found in first 500 chars)")

        # Create CSV reader
        csv_file = io.StringIO(data)

        # Parse CSV data
        try:
            logger.info(f"Starting CSV parsing with file size: {len(data)} characters")
            events = self._parse_csv_file(csv_file)
            logger.info(f"Successfully parsed {len(events)} events from CSV data")
            
            # Log a sample of the parsed events
            if events:
                sample_event = str(events[0])[:200] + "..." if len(str(events[0])) > 200 else str(events[0])
                logger.info(f"Sample parsed event: {sample_event}")
                
            return events
        except Exception as e:
            logger.error(f"Error parsing CSV data: {str(e)}")
            return []
        finally:
            csv_file.close()

    def parse_csv_file(self, file_path: str, only_new_lines: bool = False) -> List[Dict[str, Any]]:
        """Parse CSV file and return list of events

        Args:
            file_path: Path to CSV file
            only_new_lines: If True, only parse lines that haven't been processed before

        Returns:
            List[Dict]: List of parsed event dictionaries
        """
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                return self._parse_csv_file(file, file_path=file_path, only_new_lines=only_new_lines)
        except UnicodeDecodeError:
            # Try with different encoding
            with open(file_path, "r", encoding="latin-1") as file:
                return self._parse_csv_file(file, file_path=file_path, only_new_lines=only_new_lines)

    def _parse_csv_file(self, file: Union[TextIO, BinaryIO], file_path: str = None, only_new_lines: bool = False) -> List[Dict[str, Any]]:
        """Parse CSV file and return list of events

        Args:
            file: File-like object (can be text or binary)
            file_path: Path to the file (used for tracking processed lines)
            only_new_lines: If True, only parse lines that haven't been processed before

        Returns:
            List[Dict]: List of parsed event dictionaries
        """
        # Handle possible None input
        if file is None:
            logger.error(f"Null file object provided to CSV parser for {file_path or 'unknown path'}")
            return []
            
        # Check if file object has read method
        if not hasattr(file, 'read') or not callable(getattr(file, 'read')):
            logger.error(f"Invalid file object provided (no read method) for {file_path or 'unknown path'}")
            return []
            
        # Check for empty file by trying to read a sample
        try:
            file_content = file.read(4096)  # Read a larger sample to detect content
            
            # Reset file position for subsequent reads
            if hasattr(file, 'seek') and callable(getattr(file, 'seek')):
                file.seek(0)
            else:
                logger.error(f"File object does not support seek operation, cannot process {file_path or 'unknown path'}")
                return []
                
            # Handle binary content if needed
            if isinstance(file_content, bytes):
                try:
                    file_content = file_content.decode('utf-8', errors='replace')
                    logger.info(f"Converted binary content to text for {file_path or 'unknown path'}")
                except Exception as e:
                    logger.error(f"Error decoding binary content: {e}")
                    return []
                    
            # Handle unexpected content types
            if not isinstance(file_content, str):
                try:
                    file_content = str(file_content)
                    logger.warning(f"Converted non-string content of type {type(file_content)} to string")
                except Exception as e:
                    logger.error(f"Cannot convert content to string: {e}")
                    return []
        except Exception as e:
            logger.error(f"Error reading from file: {str(e)}")
            return []
        
        # If file is empty or just whitespace, return empty list
        if not file_content or not file_content.strip():
            logger.warning(f"Empty or blank CSV file detected: {file_path or 'unknown'}")
            return []
            
        # Count occurrences of potential delimiters
        delimiters = {';': 0, ',': 0, '\t': 0}
        for d in delimiters:
            delimiters[d] = file_content.count(d)
        
        # Use the most frequent delimiter if it appears significantly
        best_delimiter = self.separator  # Default
        max_count = delimiters.get(self.separator, 0)
        
        for d, count in delimiters.items():
            if count > max_count:
                max_count = count
                best_delimiter = d
        
        # If no delimiters found at all, this might not be a valid CSV
        if max_count == 0:
            logger.warning(f"No valid delimiters found in file: {file_path or 'unknown'} - content might not be CSV")
            # We'll still try to parse with default separator, but log the warning
            
        logger.info(f"Detected delimiter: '{best_delimiter}' (counts: {delimiters})")
        
        # Log first few lines for debugging
        sample_lines = file_content.split('\n')[:5]
        logger.info(f"Sample content from {file_path or 'unknown'} (first 5 lines):")
        for idx, line in enumerate(sample_lines):
            logger.info(f"Line {idx+1}: {line[:100]}{'...' if len(line) > 100 else ''}")
        
        # Reset file position
        file.seek(0)
        
        # Create CSV reader with the detected delimiter
        csv_reader = csv.reader(file, delimiter=best_delimiter)
        
        # Skip header row if present
        first_row = next(csv_reader, None)
        
        # Check if first row is header
        is_header = False
        if first_row is not None:
            # Check if first row might be column names (contains strings, not timestamps)
            first_cell = first_row[0] if first_row and len(first_row) > 0 else ""
            
            # If first field doesn't look like a date/timestamp, it might be a header
            if not re.match(r'\d{4}[-./]\d{2}[-./]\d{2}', first_cell):
                logger.info(f"First row appears to be a header: {first_row}")
                is_header = True
        
        # Reset file position if first row is not header
        if first_row is not None and not is_header:
            file.seek(0)
            csv_reader = csv.reader(file, delimiter=best_delimiter)

        # Get or initialize line counter for this file
        last_processed_line = 0
        if file_path and only_new_lines and file_path in self._last_processed_line_count:
            last_processed_line = self._last_processed_line_count[file_path]
            logger.info(f"Starting processing from line {last_processed_line} in file {file_path}")
        else:
            logger.info(f"Processing entire file {file_path or 'unknown'} (only_new_lines={only_new_lines})")
        
        # Parse rows
        events = []
        current_line = 0
        
        try:
            for row in csv_reader:
                current_line += 1
                
                # Skip already processed lines if only_new_lines is True
                if only_new_lines and current_line <= last_processed_line:
                    continue
                    
                # Skip empty rows
                if row is None or len(row) < 6:  # Minimum required fields for a kill event
                    continue
                    
                logger.debug(f"Processing row {current_line} with {len(row)} fields: {row[:3]}...")
                
                # Process the row data
                
                # Check if this might be a pre-April (7 columns) or post-April (9 columns) format
                row_format = None
                if len(row) >= 9:  # Has console fields - post-April format
                    row_format = "post_april"
                elif len(row) >= 7:  # Basic kill event - pre-April format
                    row_format = "pre_april"
                else:
                    # Unrecognized format, but we'll still try to parse with defaults
                    row_format = "unknown"
                
                # Create event dictionary
                event = {}
                for i, column in enumerate(self.columns):
                    if i < len(row):
                        event[column] = row[i].strip()
                    else:
                        # For missing fields, use appropriate defaults based on the field
                        if column in ("killer_console", "victim_console") and row_format == "pre_april":
                            event[column] = "Unknown"  # Pre-April format doesn't include console info
                        else:
                            event[column] = ""
                
                # Log format detection for debugging - uncomment if needed
                # logger.debug(f"Parsed row with format {row_format}, fields: {len(row)}, event keys: {list(event.keys())}")

                # Convert datetime column with multiple format support
                if self.datetime_column in event:
                    try:
                        # Try primary format first
                        event[self.datetime_column] = datetime.strptime(
                            event[self.datetime_column], 
                            self.datetime_format
                        )
                    except (ValueError, TypeError):
                        # Try alternative formats if primary fails
                        timestamp_str = event[self.datetime_column]
                        parsed = False

                        # Try these common formats
                        alternative_formats = [
                            "%Y.%m.%d-%H.%M.%S",      # 2025.03.27-10.42.18
                            "%Y-%m-%d %H:%M:%S",      # 2025-03-27 10:42:18
                            "%Y.%m.%d %H:%M:%S",      # 2025.03.27 10:42:18
                            "%Y/%m/%d %H:%M:%S",      # 2025/03/27 10:42:18
                            "%d.%m.%Y-%H.%M.%S",      # 27.03.2025-10.42.18
                            "%d.%m.%Y %H:%M:%S"       # 27.03.2025 10:42:18
                        ]

                        for fmt in alternative_formats:
                            try:
                                event[self.datetime_column] = datetime.strptime(timestamp_str, fmt)
                                parsed = True
                                logger.debug(f"Parsed timestamp {timestamp_str} with format {fmt}")
                                break
                            except (ValueError, TypeError):
                                continue

                        if not parsed:
                            logger.warning(f"Failed to parse timestamp: {timestamp_str}")
                            # Keep original string if all parsing attempts fail

                # Convert numeric columns
                if self.format_name == "deadside":
                    # Convert distance to float
                    if "distance" in event:
                        try:
                            event["distance"] = float(event["distance"])
                        except (ValueError, TypeError):
                            event["distance"] = 0.0

                # Add event to list
                events.append(event)
        except Exception as e:
            logger.error(f"Error in CSV processing at line {current_line}: {str(e)}")
            # Return the events that were parsed successfully so far
            return events
        
        # Update last processed line count if we're tracking this file
        if file_path and current_line > 0:
            self._last_processed_line_count[file_path] = current_line
            logger.info(f"Updated last processed line for {file_path} to {current_line}")
            
        # Log summary of what we processed
        if only_new_lines:
            logger.info(f"Processed {len(events)} new events from {current_line - last_processed_line} new lines in {file_path or 'unknown'}")
        else:
            logger.info(f"Processed {len(events)} events from entire file {file_path or 'unknown'}")

        return events

    def filter_events(self, events: List[Dict[str, Any]], 
                     start_time: Optional[datetime] = None,
                     end_time: Optional[datetime] = None,
                     player_id: Optional[str] = None,
                     min_distance: Optional[float] = None,
                     max_distance: Optional[float] = None,
                     weapon: Optional[str] = None) -> List[Dict[str, Any]]:
        """Filter events by criteria

        Args:
            events: List of events to filter
            start_time: Start time for filtering (default: None)
            end_time: End time for filtering (default: None)
            player_id: Player ID for filtering (default: None)
            min_distance: Minimum distance for filtering (default: None)
            max_distance: Maximum distance for filtering (default: None)
            weapon: Weapon name for filtering (default: None)

        Returns:
            List[Dict]: Filtered events
        """
        # Start with all events
        filtered_events = events

        # Filter by time range
        if start_time is not None or end_time:
            filtered_events = [
                event for event in filtered_events
                if (not start_time or event.get(self.datetime_column, datetime.min) >= start_time) and
                   (not end_time or event.get(self.datetime_column, datetime.max) <= end_time)
            ]

        # Filter by player ID
        if player_id is not None:
            if self.format_name == "deadside":
                filtered_events = [
                    event for event in filtered_events
                    if event.get("killer_id") == player_id or event.get("victim_id") == player_id
                ]
            elif self.format_name == "custom":
                filtered_events = [
                    event for event in filtered_events
                    if event.get("player1_id") == player_id or event.get("player2_id") == player_id
                ]

        # Filter by distance range
        if (min_distance is None or max_distance is None) and "distance" in self.columns:
            filtered_events = [
                event for event in filtered_events
                if ((min_distance is None or event.get("distance", 0) >= min_distance) and
                    (max_distance is None or event.get("distance", float("inf")) <= max_distance))
            ]

        # Filter by weapon
        if weapon is not None and "weapon" in self.columns:
            filtered_events = [
                event for event in filtered_events
                if event.get("weapon", "").lower() == weapon.lower()
            ]

        return filtered_events

    def aggregate_player_stats(self, events: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Aggregate player statistics from events

        Args:
            events: List of events

        Returns:
            Dict[str, Dict]: Dictionary of player statistics by player ID
        """
        # Initialize player stats
        player_stats = {}

        if self.format_name == "deadside":
            # Process deadside format
            for event in events:
                killer_id = event.get("killer_id")
                victim_id = event.get("victim_id")

                # Skip invalid events
                if killer_id is None or not victim_id:
                    continue

                # Extract event details
                killer_name = event.get("killer_name", "Unknown")
                victim_name = event.get("victim_name", "Unknown")
                weapon = event.get("weapon", "Unknown")
                distance = event.get("distance", 0)
                timestamp = event.get(self.datetime_column, datetime.now())

                # Extract console info if available (post-April format)
                killer_console = event.get("killer_console", "Unknown")
                victim_console = event.get("victim_console", "Unknown")

                # Update killer stats
                if killer_id is not None and killer_id not in player_stats:
                    player_stats[killer_id] = {
                        "player_id": killer_id,
                        "player_name": killer_name,
                        "kills": 0,
                        "deaths": 0,
                        "weapons": {},
                        "victims": {},
                        "killers": {},
                        "longest_kill": 0,
                        "total_distance": 0,
                        "first_seen": timestamp,
                        "last_seen": timestamp,
                        "platform": killer_console,  # Add platform/console info
                        "kills_by_platform": {}  # Track kills by victim platform
                    }

                killer_stats = player_stats[killer_id]
                killer_stats["kills"] += 1
                killer_stats["weapons"][weapon] = killer_stats["weapons"].get(weapon, 0) + 1
                killer_stats["victims"][victim_id] = killer_stats["victims"].get(victim_id, 0) + 1
                killer_stats["total_distance"] += distance
                killer_stats["longest_kill"] = max(killer_stats["longest_kill"], distance)
                killer_stats["last_seen"] = max(killer_stats["last_seen"], timestamp)

                # Track kills by victim platform (for cross-platform stats)
                if victim_console and victim_console != "Unknown":
                    killer_stats["kills_by_platform"][victim_console] = killer_stats["kills_by_platform"].get(victim_console, 0) + 1

                # Update victim stats
                if victim_id is not None and victim_id not in player_stats:
                    player_stats[victim_id] = {
                        "player_id": victim_id,
                        "player_name": victim_name,
                        "kills": 0,
                        "deaths": 0,
                        "weapons": {},
                        "victims": {},
                        "killers": {},
                        "longest_kill": 0,
                        "total_distance": 0,
                        "first_seen": timestamp,
                        "last_seen": timestamp,
                        "platform": victim_console,  # Add platform/console info
                        "deaths_by_platform": {}  # Track deaths by killer platform
                    }

                victim_stats = player_stats[victim_id]
                victim_stats["deaths"] += 1
                victim_stats["killers"][killer_id] = victim_stats["killers"].get(killer_id, 0) + 1
                victim_stats["last_seen"] = max(victim_stats["last_seen"], timestamp)

                # Track deaths by killer platform (for cross-platform stats)
                if killer_console and killer_console != "Unknown":
                    victim_stats["deaths_by_platform"][killer_console] = victim_stats["deaths_by_platform"].get(killer_console, 0) + 1

        elif self.format_name == "custom":
            # Process custom format
            pass

        # Calculate additional statistics
        for player_id, stats in player_stats.items():
            # Calculate K/D ratio
            stats["kd_ratio"] = stats["kills"] / max(stats["deaths"], 1)

            # Calculate average kill distance
            if stats["kills"] > 0:
                stats["avg_kill_distance"] = stats["total_distance"] / stats["kills"]
            else:
                stats["avg_kill_distance"] = 0

            # Calculate playtime estimate
            stats["playtime"] = (stats["last_seen"] - stats["first_seen"]).total_seconds() / 3600

            # Get favorite weapon
            if stats["weapons"]:
                stats["favorite_weapon"] = max(stats["weapons"].items(), key=lambda x: x[1])[0]
            else:
                stats["favorite_weapon"] = "None"

            # Get most killed player
            if stats["victims"]:
                most_killed_id = max(stats["victims"].items(), key=lambda x: x[1])[0]
                stats["most_killed"] = {
                    "player_id": most_killed_id,
                    "player_name": player_stats.get(most_killed_id, {}).get("player_name", "Unknown"),
                    "count": stats["victims"][most_killed_id]
                }
            else:
                stats["most_killed"] = None

            # Get nemesis (player killed by the most)
            if stats["killers"]:
                nemesis_id = max(stats["killers"].items(), key=lambda x: x[1])[0]
                stats["nemesis"] = {
                    "player_id": nemesis_id,
                    "player_name": player_stats.get(nemesis_id, {}).get("player_name", "Unknown"),
                    "count": stats["killers"][nemesis_id]
                }
            else:
                stats["nemesis"] = None

            # Calculate cross-platform statistics if platform data is available
            if "platform" in stats and stats["platform"] != "Unknown":
                # Calculate platform-specific K/D ratio
                if "kills_by_platform" in stats:
                    # Total kills by player platform
                    platform_kills = sum(stats["kills_by_platform"].values())
                    # Cross-platform K/D ratio
                    if "deaths_by_platform" in stats:
                        platform_deaths = sum(stats["deaths_by_platform"].values())
                        stats["platform_kd_ratio"] = platform_kills / max(platform_deaths, 1)

                    # Get dominant platform (platform with most kills against)
                    if stats["kills_by_platform"]:
                        stats["dominant_platform"] = max(stats["kills_by_platform"].items(), key=lambda x: x[1])[0]
                        stats["dominant_platform_kills"] = stats["kills_by_platform"][stats["dominant_platform"]]
                    else:
                        stats["dominant_platform"] = "None"
                        stats["dominant_platform_kills"] = 0

        return player_stats

    def stream_parse_csv(self, file_obj: BinaryIO, chunk_size: int = 8192) -> Generator[Dict[str, Any], None, None]:
        """Parse CSV data in streaming mode for memory-efficient processing of large files

        This method is designed to handle very large CSV files by processing them in chunks
        rather than loading the entire file into memory.

        Args:
            file_obj: Binary file-like object
            chunk_size: Size of chunks to read (default: 8KB)

        Yields:
            Dict[str, Any]: Individual parsed event records
        """
        # Initialize variables
        buffer = ""
        line_buffer = []
        detected_format = None
        tried_formats = []

        # Create CSV reader with auto-detection of dialect
        try:
            # Attempt to detect format from a sample
            sample = file_obj.read(min(chunk_size * 2, 16384))  # Read a sample (up to 16KB)
            file_obj.seek(0)  # Reset file position

            if isinstance(sample, bytes):
                sample_str = sample.decode('utf-8', errors='replace')
            else:
                sample_str = sample

            # Try to detect CSV dialect
            try:
                dialect = csv.Sniffer().sniff(sample_str, delimiters=";,\t|")
                logger.info(f"Detected CSV dialect with delimiter: {dialect.delimiter}")
            except csv.Error:
                # Fallback to default format
                logger.warning("Failed to detect CSV dialect, using default format")
                dialect = None

            # If we detected a dialect different from our configured separator, 
            # create a special format config
            if dialect is not None and dialect.delimiter != self.separator:
                logger.info(f"Adapting to detected delimiter: {dialect.delimiter}")
                detected_format = dict(self.format_config)
                detected_format["separator"] = dialect.delimiter
                tried_formats.append(detected_format)

            # Add the standard format to the list of formats to try
            tried_formats.append(self.format_config)

            # Add fallback formats if available is not None
            if "fallback_formats" in self.format_config:
                tried_formats.extend(self.format_config["fallback_formats"])

        except Exception as e:
            logger.warning(f"Error during CSV format detection: {e}")
            # Fall back to the default format
            tried_formats = [self.format_config]
            if "fallback_formats" in self.format_config:
                tried_formats.extend(self.format_config["fallback_formats"])

        # Process the file in chunks
        while True:
            # Read a chunk
            chunk = file_obj.read(chunk_size)
            if chunk is None:
                break  # End of file

            # Decode chunk
            if isinstance(chunk, bytes):
                try:
                    chunk_str = chunk.decode('utf-8', errors='replace')
                except UnicodeDecodeError:
                    chunk_str = chunk.decode('latin-1', errors='replace')
            else:
                chunk_str = chunk

            # Append to buffer
            buffer += chunk_str

            # Split buffer by newlines
            lines = buffer.split('\n')
            buffer = lines.pop()  # Keep last partial line in buffer

            # Add complete lines to line buffer
            line_buffer.extend(lines)

            # Process lines when we have enough data
            while line_buffer:
                line = line_buffer.pop(0)
                if not line or not line.strip():
                    continue  # Skip empty lines

                # Try all formats until one works
                parsed_record = None
                for format_config in tried_formats:
                    try:
                        separator = format_config.get("separator", self.separator)
                        columns = format_config.get("columns", self.columns)
                        datetime_column = format_config.get("datetime_column", self.datetime_column)
                        datetime_format = format_config.get("datetime_format", self.datetime_format)

                        # Parse this line
                        fields = line.split(separator)
                        if len(fields) < 3:  # Minimum required fields
                            continue

                        # Create event record
                        record = {}
                        for i, column in enumerate(columns):
                            if i < len(fields):
                                record[column] = fields[i].strip()
                            else:
                                record[column] = ""

                        # Check required columns (if specified)
                        if "required_columns" in format_config:
                            required_ok = True
                            for req_col in format_config["required_columns"]:
                                if not record or not record.get(req_col):
                                    required_ok = False
                                    break

                            if not required_ok:
                                continue  # Skip to next format

                        # Convert datetime
                        if datetime_column in record and record[datetime_column]:
                            try:
                                record[datetime_column] = datetime.strptime(
                                    record[datetime_column], 
                                    datetime_format
                                )
                            except (ValueError, TypeError):
                                # Keep original string
                                pass

                        # Parse numeric fields
                        if "distance" in record:
                            try:
                                record["distance"] = float(record["distance"])
                            except (ValueError, TypeError):
                                record["distance"] = 0.0

                        # Successful parse
                        parsed_record = record

                        # If we found a working format, stick with it for optimization
                        if tried_formats[0] != format_config:
                            logger.info(f"Switching to working format with separator: {separator}")
                            tried_formats.insert(0, format_config)

                        break  # Exit the format loop

                    except Exception as e:
                        # Try next format
                        continue

                # Yield the parsed record if successful is not None
                if parsed_record is not None:
                    yield parsed_record

        # Process any remaining line in the buffer
        if buffer.strip():
            for format_config in tried_formats:
                try:
                    separator = format_config.get("separator", self.separator)
                    columns = format_config.get("columns", self.columns)
                    datetime_column = format_config.get("datetime_column", self.datetime_column)
                    datetime_format = format_config.get("datetime_format", self.datetime_format)

                    # Parse this line
                    fields = buffer.split(separator)
                    if len(fields) < 3:  # Minimum required fields
                        continue

                    # Create event record
                    record = {}
                    for i, column in enumerate(columns):
                        if i < len(fields):
                            record[column] = fields[i].strip()
                        else:
                            record[column] = ""

                    # Check required columns (if specified)
                    if "required_columns" in format_config:
                        required_ok = True
                        for req_col in format_config["required_columns"]:
                            if not record or not record.get(req_col):
                                required_ok = False
                                break

                        if not required_ok:
                            continue  # Skip to next format

                    # Convert datetime
                    if datetime_column in record and record[datetime_column]:
                        try:
                            record[datetime_column] = datetime.strptime(
                                record[datetime_column], 
                                datetime_format
                            )
                        except (ValueError, TypeError):
                            # Keep original string
                            pass

                    # Parse numeric fields
                    if "distance" in record:
                        try:
                            record["distance"] = float(record["distance"])
                        except (ValueError, TypeError):
                            record["distance"] = 0.0

                    # Yield the parsed record
                    yield record
                    break  # Exit the format loop

                except Exception:
                    # Try next format
                    continue

    def detect_format(self, file_obj: BinaryIO) -> Dict[str, Any]:
        """Detect CSV format from file content, including pre-April vs post-April formats

        Args:
            file_obj: Binary file-like object

        Returns:
            Dict[str, Any]: Detected format configuration
        """
        # Remember current position
        current_pos = file_obj.tell()

        try:
            # Read a sample
            sample = file_obj.read(8192)  # 8KB sample
            if sample is None:
                return self.format_config  # Empty file, use default

            # Decode sample
            if isinstance(sample, bytes):
                try:
                    sample_str = sample.decode('utf-8', errors='replace')
                except UnicodeDecodeError:
                    sample_str = sample.decode('latin-1', errors='replace')
            else:
                sample_str = sample

            # Try to detect dialect
            try:
                dialect = csv.Sniffer().sniff(sample_str, delimiters=";,\t|")
                logger.info(f"Detected delimiter: {dialect.delimiter}")

                # Store format detection for later use in the object
                if not hasattr(self, "detected_format_info"):
                    self.detected_format_info = {}

                # Create format config with detected delimiter
                format_config = dict(self.format_config)
                format_config["separator"] = dialect.delimiter

                # Determine pre-April vs post-April format
                # Check the first line to determine the number of fields
                first_line = sample_str.split('\n')[0].strip()
                if dialect.delimiter in first_line:
                    parts = first_line.split(dialect.delimiter)
                    num_fields = len(parts)

                    # Check for post-April format (with console fields)
                    if num_fields >= 9:
                        logger.info("Detected post-April CSV format (9 columns with console info)")
                        self.detected_format_info["april_update"] = True

                        # Update columns if needed
                        if "killer_console" not in format_config["columns"] and len(format_config["columns"]) < 9:
                            format_config["columns"] = format_config["columns"][:7] + ["killer_console", "victim_console"]

                    # Check for pre-April format (7 fields)
                    elif num_fields >= 7:
                        logger.info("Detected pre-April CSV format (7 columns)")
                        self.detected_format_info["april_update"] = False

                return format_config

            except csv.Error:
                logger.warning("Could not detect CSV dialect, using default format")
                return self.format_config

        except Exception as e:
            logger.error(f"Error detecting CSV format: {e}")
            return self.format_config

        finally:
            # Reset file position
            file_obj.seek(current_pos)

    def get_leaderboard(self, player_stats: Dict[str, Dict[str, Any]], stat_name: str, limit: int = 10, 
                      platform: Optional[str] = None) -> List[Dict[str, Any]]:
        """Generate leaderboard from player statistics, optionally filtered by platform

        Args:
            player_stats: Dictionary of player statistics by player ID
            stat_name: Statistic name to rank by
            limit: Maximum number of entries (default: 10)
            platform: Optional platform to filter by (e.g., "PS5", "Xbox", "PC")

        Returns:
            List[Dict]: Leaderboard entries
        """
        # Filter players by platform if specified
        if platform:
            filtered_players = [
                stats for _, stats in player_stats.items()
                if stats.get("platform") == platform
            ]
        else:
            filtered_players = [stats for _, stats in player_stats.items()]

        # Sort players by statistic
        sorted_players = sorted(
            filtered_players,
            key=lambda x: x.get(stat_name, 0),
            reverse=True
        )

        # Create leaderboard entries
        leaderboard = []
        for i, player in enumerate(sorted_players[:limit]):
            entry = {
                "rank": i + 1,
                "player_id": player["player_id"],
                "player_name": player["player_name"],
                "value": player.get(stat_name, 0)
            }

            # Include platform info if available
            if "platform" in player:
                entry["platform"] = player["platform"]

            leaderboard.append(entry)

        return leaderboard

    def get_platform_comparison(self, player_stats: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Generate cross-platform comparison statistics

        Args:
            player_stats: Dictionary of player statistics by player ID

        Returns:
            Dict[str, Any]: Platform comparison statistics
        """
        # Initialize platform stats
        platforms = {}
        platform_kills = {}
        platform_deaths = {}

        # Aggregate platform statistics
        for player_id, stats in player_stats.items():
            # Skip players with unknown platform
            if "platform" not in stats or stats["platform"] == "Unknown":
                continue

            platform = stats["platform"]

            # Initialize platform entry
            if platform not in platforms:
                platforms[platform] = {
                    "total_players": 0,
                    "total_kills": 0,
                    "total_deaths": 0,
                    "avg_kd": 0.0,
                    "kills_by_victim_platform": {},
                    "deaths_by_killer_platform": {}
                }

            # Update platform stats
            platforms[platform]["total_players"] += 1
            platforms[platform]["total_kills"] += stats.get("kills", 0)
            platforms[platform]["total_deaths"] += stats.get("deaths", 0)

            # Process cross-platform kills
            if "kills_by_platform" in stats:
                for victim_platform, kill_count in stats["kills_by_platform"].items():
                    if victim_platform not in platforms[platform]["kills_by_victim_platform"]:
                        platforms[platform]["kills_by_victim_platform"][victim_platform] = 0
                    platforms[platform]["kills_by_victim_platform"][victim_platform] += kill_count

            # Process cross-platform deaths
            if "deaths_by_platform" in stats:
                for killer_platform, death_count in stats["deaths_by_platform"].items():
                    if killer_platform not in platforms[platform]["deaths_by_killer_platform"]:
                        platforms[platform]["deaths_by_killer_platform"][killer_platform] = 0
                    platforms[platform]["deaths_by_killer_platform"][killer_platform] += death_count

        # Calculate averages and additional statistics
        for platform, data in platforms.items():
            # Calculate average K/D ratio for platform
            if data["total_deaths"] > 0:
                data["avg_kd"] = data["total_kills"] / data["total_deaths"]
            else:
                data["avg_kd"] = data["total_kills"]  # Avoid division by zero

            # Calculate dominant victim platform
            if data["kills_by_victim_platform"]:
                data["dominant_victim_platform"] = max(
                    data["kills_by_victim_platform"].items(),
                    key=lambda x: x[1]
                )[0]
            else:
                data["dominant_victim_platform"] = "None"

            # Calculate dominant killer platform
            if data["deaths_by_killer_platform"]:
                data["dominant_killer_platform"] = max(
                    data["deaths_by_killer_platform"].items(),
                    key=lambda x: x[1]
                )[0]
            else:
                data["dominant_killer_platform"] = "None"

        return platforms

    def detect_format_from_string(self, data: Union[str, bytes]) -> str:
        """Detect log format from string data, identifying pre-April vs post-April format

        Args:
            data: CSV data string or bytes

        Returns:
            str: Detected format name
        """
        # Convert bytes to string if needed
        if isinstance(data, bytes):
            data = data.decode("utf-8", errors="replace")

        # Create CSV reader for each format
        csv_file = io.StringIO(data)

        try:
            # Get first line
            first_line = csv_file.readline().strip()

            # Reset file position
            csv_file.seek(0)

            # Store format detection for later use in the object
            if not hasattr(self, "detected_format_info"):
                self.detected_format_info = {}

            # Check semicolon separator (deadside)
            if ";" in first_line:
                parts = first_line.split(";")
                num_fields = len(parts)

                # Check number of fields to determine pre or post-April format
                if num_fields >= 9:
                    # Post-April format with console fields
                    logger.info("Detected post-April CSV format (9 columns with console info)")
                    self.detected_format_info["april_update"] = True
                    return "deadside"
                elif num_fields >= 7:
                    # Pre-April format
                    logger.info("Detected pre-April CSV format (7 columns)")
                    self.detected_format_info["april_update"] = False
                    return "deadside"

                # Try to detect by timestamp pattern
                if parts and (re.match(r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}", parts[0]) or
                             re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", parts[0])):
                    logger.info(f"Detected deadside format with timestamp pattern: {parts[0]}")
                    return "deadside"

            # Check comma separator (custom)
            if "," in first_line and len(first_line.split(",")) >= 6:
                separator = ","
                parts = first_line.split(",")

                # Check for timestamp format
                if re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", parts[0]):
                    return "custom"

            # Default to deadside
            logger.warning(f"Could not determine format precisely, defaulting to deadside (line: {first_line[:50]}...)")
            return "deadside"

        finally:
            csv_file.close()

    def add_custom_format(self, format_name: str, format_config: Dict[str, Any]) -> None:
        """Add custom log format

        Args:
            format_name: Format name
            format_config: Format configuration
        """
        # Validate format configuration
        required_keys = ["separator", "columns", "datetime_format", "datetime_column"]
        for key in required_keys:
            if key not in format_config:
                raise ValueError(f"Missing required key in format config: {key}")

        # Add format to LOG_FORMATS
        self.LOG_FORMATS[format_name] = format_config

        # Update current format if matching is not None
        if format_name == self.format_name:
            self.format_config = format_config
            self.separator = format_config["separator"]
            self.columns = format_config["columns"]
            self.datetime_format = format_config["datetime_format"]
            self.datetime_column = format_config["datetime_column"]