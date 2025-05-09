"""
Parser utilities for Tower of Temptation PvP Statistics Discord Bot.

This module provides:
1. Integration of the three-part parsing system:
   - Historical CSV parser (one-time parsing of historical data)
   - 5-minute automatic CSV parser (ongoing parsing of CSV files)
   - Deadside.log parser (real-time event processing)
2. Helper functions for normalizing data between parsers
3. Utilities for ensuring parser coordination and avoiding duplicates
"""
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Set, Tuple

from utils.csv_parser import CSVParser
from utils.log_parser import LogParser, parse_log_file
from utils.path_utils import (
    get_base_path,
    get_log_path,
    get_csv_path,
    get_log_file_path
)

logger = logging.getLogger(__name__)

class ParserCoordinator:
    """Coordinates between the three parser subsystems to avoid duplicate events"""
    
    def __init__(self):
        """Initialize parser coordinator"""
        self.last_processed_csv_timestamps = {}  # server_id -> timestamp
        self.last_processed_log_timestamps = {}  # server_id -> timestamp
        self.processed_event_hashes = set()  # Set to track processed event hashes
        self.recent_event_window = 3600  # 1 hour window for deduplication
        
    def generate_event_hash(self, event: Dict[str, Any]) -> str:
        """Generate a hash for an event to check for duplicates
        
        Args:
            event: Event dictionary
            
        Returns:
            str: Hash string
        """
        # For kill events
        if "killer_id" in event and "victim_id" in event:
            timestamp = event.get("timestamp", "")
            if isinstance(timestamp, datetime):
                timestamp = timestamp.isoformat()
                
            # Create a unique string from key event properties
            hash_string = f"{timestamp}_{event.get('killer_id', '')}_{event.get('victim_id', '')}_{event.get('weapon', '')}"
            return hash_string
            
        # For mission events
        elif "event_type" in event and event.get("event_type") == "mission":
            timestamp = event.get("timestamp", "")
            if isinstance(timestamp, datetime):
                timestamp = timestamp.isoformat()
                
            hash_string = f"{timestamp}_{event.get('mission_name', '')}_{event.get('location', '')}"
            return hash_string
            
        # For other game events (airdrop, helicrash, etc.)
        elif "event_type" in event and event.get("event_type") in ["airdrop", "helicrash", "trader", "convoy"]:
            timestamp = event.get("timestamp", "")
            if isinstance(timestamp, datetime):
                timestamp = timestamp.isoformat()
                
            hash_string = f"{timestamp}_{event.get('event_type', '')}_{event.get('event_id', '')}"
            return hash_string
            
        # For connection events
        elif "event_type" in event and event.get("event_type") in ["register", "unregister", "join", "kick"]:
            timestamp = event.get("timestamp", "")
            if isinstance(timestamp, datetime):
                timestamp = timestamp.isoformat()
                
            hash_string = f"{timestamp}_{event.get('event_type', '')}_{event.get('player_id', '')}"
            return hash_string
            
        # Fallback for unknown event types
        else:
            timestamp = event.get("timestamp", "")
            if isinstance(timestamp, datetime):
                timestamp = timestamp.isoformat()
                
            return f"{timestamp}_{hash(str(event))}"
    
    def is_duplicate_event(self, event: Dict[str, Any]) -> bool:
        """Check if an is not None event has already been processed
        
        Args:
            event: Event dictionary
            
        Returns:
            bool: True if duplicate, False otherwise
        """
        event_hash = self.generate_event_hash(event)
        
        if event_hash in self.processed_event_hashes:
            return True
            
        # Add to processed set
        self.processed_event_hashes.add(event_hash)
        
        # Keep the set from growing too large by periodically pruning old entries
        self._prune_old_hashes()
        
        return False
    
    def _prune_old_hashes(self):
        """Remove old event hashes to prevent memory growth"""
        # Every 1000 events, check if we is not None should prune
        if len(self.processed_event_hashes) > 10000:
            # Keep only the most recent 1000 events
            # In a real implementation this would be time-based, but
            # that would require storing timestamps with each hash
            if len(self.processed_event_hashes) > 1000:
                self.processed_event_hashes = set(list(self.processed_event_hashes)[-1000:])
    
    def update_csv_timestamp(self, server_id: str, timestamp: datetime):
        """Update last processed CSV timestamp for a server
        
        Args:
            server_id: Server ID
            timestamp: Last processed timestamp
        """
        self.last_processed_csv_timestamps[server_id] = timestamp
    
    def update_log_timestamp(self, server_id: str, timestamp: datetime):
        """Update last processed log timestamp for a server
        
        Args:
            server_id: Server ID
            timestamp: Last processed timestamp
        """
        self.last_processed_log_timestamps[server_id] = timestamp
    
    def get_last_csv_timestamp(self, server_id: str) -> Optional[datetime]:
        """Get last processed CSV timestamp for a server
        
        Args:
            server_id: Server ID
            
        Returns:
            datetime or None: Last processed timestamp
        """
        return self.last_processed_csv_timestamps.get(server_id)
    
    def get_last_log_timestamp(self, server_id: str) -> Optional[datetime]:
        """Get last processed log timestamp for a server
        
        Args:
            server_id: Server ID
            
        Returns:
            datetime or None: Last processed timestamp
        """
        return self.last_processed_log_timestamps.get(server_id)
    
    def should_process_csv(self, server_id: str, csv_timestamp: datetime) -> bool:
        """Check if a is not None CSV file should be processed
        
        Args:
            server_id: Server ID
            csv_timestamp: Timestamp of the CSV file
            
        Returns:
            bool: True if should is not None process, False otherwise
        """
        last_timestamp = self.get_last_csv_timestamp(server_id)
        
        # If we haven't processed any CSV for this server, process it
        if last_timestamp is None:
            return True
            
        # Process only if it's newer than the last processed timestamp
        return csv_timestamp > last_timestamp
    
    def should_process_log(self, server_id: str, log_timestamp: datetime) -> bool:
        """Check if a is not None log entry should be processed
        
        Args:
            server_id: Server ID
            log_timestamp: Timestamp of the log entry
            
        Returns:
            bool: True if should is not None process, False otherwise
        """
        last_timestamp = self.get_last_log_timestamp(server_id)
        
        # If we haven't processed any logs for this server, process it
        if last_timestamp is None:
            return True
            
        # Process only if it's newer than the last processed timestamp
        return log_timestamp > last_timestamp

# Create a global coordinator instance
parser_coordinator = ParserCoordinator()

def normalize_event_data(event: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize event data from different parser sources
    
    Ensures consistent field names and data types across all parser outputs.
    This function handles both pre-April and post-April CSV formats,
    ensuring consistent data structure regardless of source format.
    
    Args:
        event: Raw event dictionary
        
    Returns:
        Dict: Normalized event dictionary
    """
    normalized = event.copy()
    
    # Ensure timestamp is datetime
    if "timestamp" in normalized:
        timestamp = normalized["timestamp"]
        if isinstance(timestamp, str):
            try:
                # Try ISO format first
                normalized["timestamp"] = datetime.fromisoformat(timestamp)
            except ValueError:
                try:
                    # Try other common formats
                    common_formats = [
                        "%Y.%m.%d-%H.%M.%S",
                        "%Y.%m.%d-%H.%M.%S:%f",
                        "%Y-%m-%d %H:%M:%S",
                        "%Y-%m-%d %H:%M:%S.%f"
                    ]
                    
                    for fmt in common_formats:
                        try:
                            normalized["timestamp"] = datetime.strptime(timestamp, fmt)
                            break
                        except ValueError:
                            continue
                            
                    # If we still haven't parsed it, use current time
                    if isinstance(normalized["timestamp"], str):
                        logger.warning(f"Could not parse timestamp: {timestamp}")
                        normalized["timestamp"] = datetime.utcnow()
                except Exception as e:
                    logger.error(f"Error parsing timestamp '{timestamp}': {e}")
                    normalized["timestamp"] = datetime.utcnow()
    else:
        # If no timestamp, add current time
        normalized["timestamp"] = datetime.utcnow()
    
    # Normalize player identifiers
    if "killer_id" in normalized and normalized["killer_id"] is None:
        normalized["killer_id"] = ""
        
    # Handle console fields for pre-April format (7 fields)
    # If this is a pre-April format event (no console fields), add default values
    if "killer_id" in normalized and "victim_id" in normalized:
        # Check if this is a pre-April format (missing console fields)
        if "killer_console" not in normalized and "victim_console" not in normalized:
            logger.debug("Adding default console fields to pre-April format event")
            normalized["killer_console"] = "Unknown"
            normalized["victim_console"] = "Unknown"
        # Ensure console fields are never None
        elif normalized.get("killer_console") is None:
            normalized["killer_console"] = "Unknown"
        elif normalized.get("victim_console") is None:
            normalized["victim_console"] = "Unknown"
            
        # Also handle 'suicide_by_relocation' and other special weapon indicators
        # that were added in the post-April format
        if normalized.get("weapon") == "suicide_by_relocation" or normalized.get("weapon") == "suicide":
            # This is a suicide event (specific to post-April format)
            if normalized.get("killer_id") == normalized.get("victim_id"):
                logger.debug("Detected suicide event based on weapon identifier")
                normalized["event_type"] = "suicide"
            else:
                # Handle edge case: if victim==killer but IDs don't match (data inconsistency)
                if normalized.get("killer_name") == normalized.get("victim_name"):
                    logger.debug("Detected potential suicide event (names match but IDs don't)")
                    normalized["event_type"] = "suicide"
                    # Correct potentially inconsistent data
                    normalized["killer_id"] = normalized.get("victim_id")
        elif normalized.get("killer_id") == normalized.get("victim_id"):
            # Even if the weapon doesn't have suicide indicator, it's still a suicide if killer==victim
            logger.debug("Detected implied suicide (killer_id == victim_id)")
            normalized["event_type"] = "suicide"
        else:
            # Regular kill event
            normalized["event_type"] = "kill"
        
    if "victim_id" in normalized and normalized["victim_id"] is None:
        normalized["victim_id"] = ""
        
    if "player_id" in normalized and normalized["player_id"] is None:
        normalized["player_id"] = ""
    
    # Normalize string fields
    for field in ["killer_name", "victim_name", "player_name", "weapon", "location"]:
        if field in normalized and normalized[field] is None:
            normalized[field] = ""
    
    # Normalize numeric fields
    for field in ["distance"]:
        if field in normalized and normalized[field] is None:
            normalized[field] = 0
            
        # Convert string distances to integers
        if field in normalized and isinstance(normalized[field], str):
            try:
                normalized[field] = int(float(normalized[field]))
            except (ValueError, TypeError):
                normalized[field] = 0
    
    return normalized

def categorize_event(event: Dict[str, Any]) -> str:
    """Categorize an event by type
    
    This enhanced function handles both pre-April and post-April CSV formats,
    with special handling for suicide detection across both formats.
    
    Args:
        event: Event dictionary
        
    Returns:
        str: Event category (kill, suicide, connection, mission, game_event)
    """
    # If event has an explicit type set by normalize_event_data, use it as priority
    if "event_type" in event:
        event_type = event["event_type"]
        # If it's already categorized as kill or suicide by the normalizer
        if event_type in ["kill", "suicide"]:
            return event_type
        # Other event types
        elif event_type in ["register", "unregister", "join", "kick"]:
            return "connection"
        elif event_type == "mission":
            return "mission"
        elif event_type in ["airdrop", "helicrash", "trader", "convoy"]:
            return "game_event"
    
    # Check for post-April format suicide indicators
    # These are specific to the post-April format with 9 fields
    if "weapon" in event:
        weapon = event.get("weapon", "").lower()
        if weapon in ["suicide_by_relocation", "suicide", "suicide_fall"]:
            # Log detection of post-April suicide format
            logger.debug(f"Detected post-April format suicide by weapon: {weapon}")
            return "suicide"
    
    # Kill events have killer and victim fields (both formats)
    if "killer_id" in event and "victim_id" in event:
        killer_id = event.get("killer_id", "")
        victim_id = event.get("victim_id", "")
        
        # Check for suicide (by matching IDs) - works in both formats
        if killer_id and victim_id and killer_id == victim_id:
            logger.debug(f"Detected suicide by matching IDs: {killer_id}")
            return "suicide"
            
        # Check for suicide (by matching names if IDs differ) - data inconsistency edge case
        if "killer_name" in event and "victim_name" in event:
            killer_name = event.get("killer_name", "")
            victim_name = event.get("victim_name", "")
            if killer_name and victim_name and killer_name == victim_name:
                logger.debug(f"Detected potential suicide by matching names: {killer_name}")
                return "suicide"
        
        # Check for empty weapon field in suicide cases
        if "weapon" in event and not event.get("weapon"):
            if killer_id == victim_id:
                logger.debug("Detected suicide with empty weapon field")
                return "suicide"
        
        # Detect players trying to commit suicide by special methods (looking for keywords)
        if "weapon" in event:
            weapon = str(event.get("weapon", "")).lower()
            suicide_keywords = ["suicide", "fall", "falling", "drown", "drowning", "relog", "relocation"]
            for keyword in suicide_keywords:
                if keyword in weapon:
                    logger.debug(f"Detected suicide by keyword in weapon: {weapon}")
                    return "suicide"
        
        # If we've gotten this far, it's probably a regular kill
        # For both pre-April and post-April formats
        return "kill"
        
    # Connection events have player_id and action fields
    if "player_id" in event and "action" in event:
        return "connection"
        
    # Unknown event type
    logger.debug(f"Unknown event type: {event}")
    return "unknown"