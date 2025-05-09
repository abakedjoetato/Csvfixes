"""
Parser utilities for normalizing and categorizing log events.

This module provides functions for:
1. Normalizing event data from different sources
2. Categorizing events based on their characteristics
3. Detecting suicides and other special event types
4. Coordinating parser state between cogs
"""
import logging
import re
from datetime import datetime
from typing import Dict, Any, Optional, List, Union

logger = logging.getLogger(__name__)

# Parser coordinator singleton for sharing state between parser instances
class ParserCoordinator:
    """Parser coordinator for sharing state between parser instances"""
    def __init__(self):
        self.last_csv_timestamps = {}  # server_id -> last timestamp
        self.last_log_timestamps = {}  # server_id -> last timestamp
        self.processed_file_counts = {}  # server_id -> count
        self.processed_event_counts = {}  # server_id -> count
        
    def update_csv_timestamp(self, server_id: str, timestamp: datetime) -> None:
        """Update the last CSV timestamp for a server"""
        if server_id not in self.last_csv_timestamps or timestamp > self.last_csv_timestamps[server_id]:
            self.last_csv_timestamps[server_id] = timestamp
            
    def update_log_timestamp(self, server_id: str, timestamp: datetime) -> None:
        """Update the last log timestamp for a server"""
        if server_id not in self.last_log_timestamps or timestamp > self.last_log_timestamps[server_id]:
            self.last_log_timestamps[server_id] = timestamp
            
    def increment_processed_files(self, server_id: str, count: int = 1) -> None:
        """Increment the processed file count for a server"""
        self.processed_file_counts[server_id] = self.processed_file_counts.get(server_id, 0) + count
        
    def increment_processed_events(self, server_id: str, count: int = 1) -> None:
        """Increment the processed event count for a server"""
        self.processed_event_counts[server_id] = self.processed_event_counts.get(server_id, 0) + count
        
    def get_csv_timestamp(self, server_id: str) -> Optional[datetime]:
        """Get the last CSV timestamp for a server"""
        return self.last_csv_timestamps.get(server_id)
        
    def get_log_timestamp(self, server_id: str) -> Optional[datetime]:
        """Get the last log timestamp for a server"""
        return self.last_log_timestamps.get(server_id)
        
    def get_processed_files(self, server_id: str) -> int:
        """Get the processed file count for a server"""
        return self.processed_file_counts.get(server_id, 0)
        
    def get_processed_events(self, server_id: str) -> int:
        """Get the processed event count for a server"""
        return self.processed_event_counts.get(server_id, 0)
        
    def get_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all servers"""
        stats = {}
        for server_id in set(list(self.last_csv_timestamps.keys()) + 
                          list(self.last_log_timestamps.keys())):
            stats[server_id] = {
                "last_csv_timestamp": self.last_csv_timestamps.get(server_id),
                "last_log_timestamp": self.last_log_timestamps.get(server_id),
                "processed_files": self.processed_file_counts.get(server_id, 0),
                "processed_events": self.processed_event_counts.get(server_id, 0)
            }
        return stats

# Create a singleton instance
parser_coordinator = ParserCoordinator()

def normalize_event_data(event: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize event data to a standard format
    
    Args:
        event: Event data from parser
        
    Returns:
        Dict[str, Any]: Normalized event data with consistent keys
    """
    if not event:
        return {}
        
    # Create a new dict for normalized data
    normalized = {}
    
    # Map known field names to standard names
    field_mapping = {
        "timestamp": ["timestamp", "date", "time", "datetime", "event_time"],
        "killer_name": ["killer_name", "attacker_name", "player1_name", "source"],
        "killer_id": ["killer_id", "attacker_id", "player1_id", "source_id"],
        "victim_name": ["victim_name", "target_name", "player2_name", "target"],
        "victim_id": ["victim_id", "target_id", "player2_id", "target_id"],
        "weapon": ["weapon", "weapon_name", "item", "cause", "details"],
        "distance": ["distance", "range", "length"],
        "platform": ["platform", "killer_platform", "victim_platform"],
        "killer_platform": ["killer_platform", "killer_console"],
        "victim_platform": ["victim_platform", "victim_console"],
        "map": ["map", "location", "area"],
        "server_id": ["server_id", "server", "game_server"],
    }
    
    # Apply field mapping
    for standard_name, aliases in field_mapping.items():
        for alias in aliases:
            if alias in event:
                normalized[standard_name] = event[alias]
                break
    
    # Copy any other fields not in the mapping
    for key, value in event.items():
        if key not in [alias for aliases in field_mapping.values() for alias in aliases]:
            normalized[key] = value
    
    # Handle distance conversion to float
    if "distance" in normalized:
        try:
            normalized["distance"] = float(normalized["distance"])
        except (ValueError, TypeError):
            normalized["distance"] = 0.0
    
    # Add generated event ID if missing
    if "id" not in normalized:
        parts = [
            str(normalized.get("timestamp", "")),
            str(normalized.get("killer_name", "")),
            str(normalized.get("victim_name", "")),
            str(normalized.get("weapon", ""))
        ]
        normalized["id"] = ":".join(parts)
    
    return normalized

def detect_suicide(normalized_event: Dict[str, Any]) -> bool:
    """Detect if an event is a suicide
    
    Args:
        normalized_event: Normalized event data
        
    Returns:
        bool: True if event is a suicide
    """
    # Method 1: Direct suicide field check
    if "is_suicide" in normalized_event:
        return bool(normalized_event["is_suicide"])
    
    # Extract relevant fields for suicide detection
    killer_id = normalized_event.get("killer_id")
    victim_id = normalized_event.get("victim_id")
    killer_name = normalized_event.get("killer_name", "").lower()
    victim_name = normalized_event.get("victim_name", "").lower()
    weapon = normalized_event.get("weapon", "").lower()
    
    # Method 2: Same player ID
    if killer_id and victim_id and killer_id == victim_id:
        return True
    
    # Method 3: Empty killer ID with valid victim ID
    if not killer_id and victim_id:
        # Check for suicide weapons
        suicide_weapons = ["suicide", "fall", "falldamage", "thirst", "hunger", "malarky", "zombie", "radiation"]
        if any(w in weapon for w in suicide_weapons):
            return True
    
    # Method 4: Same player name but different or missing IDs
    if killer_name and victim_name and killer_name == victim_name:
        return True
    
    # Method 5: Suicide weapon names
    explicit_suicide_terms = ["suicide", "killed themselves", "took their own life"]
    if any(term in weapon for term in explicit_suicide_terms):
        return True
    
    # Method 6: Environment deaths
    environment_terms = ["environment", "world", "game", "fall damage", "falldamage", "radiation"]
    if killer_name in environment_terms or any(term in weapon for term in environment_terms):
        return True
    
    return False

def categorize_event(normalized_event: Dict[str, Any]) -> str:
    """Categorize an event based on its characteristics
    
    Args:
        normalized_event: Normalized event data
        
    Returns:
        str: Event category
    """
    # Get the explicit event type if available
    if "event_type" in normalized_event:
        return normalized_event["event_type"]
    
    # Detect suicides
    if detect_suicide(normalized_event):
        return "suicide"
    
    # Check if we have the minimum required fields for a kill event
    required_fields = ["victim_name", "timestamp"]
    if all(field in normalized_event for field in required_fields):
        # If we have a killer, it's a kill event
        if "killer_name" in normalized_event:
            return "kill"
        # Otherwise it's a death event
        else:
            return "death"
    
    # If we can't categorize it, return unknown
    return "unknown"