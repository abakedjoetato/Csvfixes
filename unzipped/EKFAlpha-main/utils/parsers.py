"""
Parsers for CSV and log files
"""
import re
import logging
import datetime
from typing import List, Dict, Any, Tuple, Optional

from config import CSV_FIELDS, EVENT_PATTERNS

logger = logging.getLogger(__name__)

class CSVParser:
    """Parser for CSV kill data files"""
    
    @staticmethod
    def normalize_weapon_name(weapon: str) -> str:
        """Normalize weapon names to ensure consistency
        
        This function standardizes weapon names by correcting common variations,
        typos, and ensuring consistent capitalization and formatting.
        
        Args:
            weapon: The weapon name from the CSV
            
        Returns:
            Normalized weapon name
        """
        if weapon is None or not weapon.strip():
            return "Unknown"
            
        # Convert to lowercase for easier matching
        weapon_lower = weapon.lower().strip()
        
        # Define weapon name corrections and aliases
        weapon_corrections = {
            # Rifles
            "akm": "AKM",
            "ak-47": "AKM",
            "ak47": "AKM",
            "ak74": "AK-74",
            "ak-74": "AK-74",
            "m4": "M4",
            "m4a1": "M4",
            "m16": "M16",
            "m16a4": "M16",
            "fal": "FAL",
            "sks": "SKS",
            
            # SMGs
            "bizon": "PP-19 Bizon",
            "pp-19": "PP-19 Bizon",
            "pp19": "PP-19 Bizon",
            "pp_19": "PP-19 Bizon",
            "pp-19bizon": "PP-19 Bizon",
            "mp5": "MP5",
            "mp-5": "MP5",
            "mp_5": "MP5",
            "ump": "UMP-45",
            "ump45": "UMP-45",
            "ump-45": "UMP-45",
            "vector": "Vector",
            
            # Shotguns
            "shotgun": "Shotgun",
            "saiga": "Saiga-12",
            "saiga12": "Saiga-12",
            "saiga-12": "Saiga-12",
            "pump": "Pump Shotgun",
            "pump shotgun": "Pump Shotgun",
            
            # Pistols
            "pm": "PM",
            "makarov": "PM",
            "1911": "1911",
            "colt": "1911",
            "colt1911": "1911",
            "desert eagle": "Desert Eagle",
            "deserteagle": "Desert Eagle",
            "deagle": "Desert Eagle",
            "glock": "Glock",
            "glock19": "Glock",
            
            # Snipers
            "svd": "SVD",
            "dragunov": "SVD",
            "m24": "M24",
            "mosin": "Mosin",
            "mosin-nagant": "Mosin",
            
            # Special
            "falling": "Falling",
            "suicide_by_relocation": "Suicide (Menu)",
            "suicide": "Suicide",
            "vehicle": "Vehicle",
            "land_vehicle": "Land Vehicle",
            "boat": "Boat",
            "grenade": "Grenade",
            "explosion": "Explosion",
            "fire": "Fire",
            "bleeding": "Bleeding",
            "starvation": "Starvation",
            "dehydration": "Dehydration",
            "cold": "Cold",
            "zombie": "Zombie",
            "fists": "Fists",
            "melee": "Melee",
            "knife": "Knife",
        }
        
        # Check for exact matches in our corrections dictionary
        if weapon_lower in weapon_corrections:
            return weapon_corrections[weapon_lower]
            
        # Check for partial matches
        for partial, normalized in weapon_corrections.items():
            if partial in weapon_lower:
                return normalized
                
        # If no match found, just capitalize the first letter of each word
        # and remove any extra spaces
        return " ".join(w.capitalize() for w in weapon.split())
    
    @staticmethod
    def parse_kill_line(line: str) -> Optional[Dict[str, Any]]:
        """Parse a single line from a CSV file into a kill event with improved error tolerance
        
        Handles both pre-April (7-field) and post-April (9-field) CSV formats:
        - Pre-April: timestamp;killer_name;killer_id;victim_name;victim_id;weapon;distance
        - Post-April: timestamp;killer_name;killer_id;victim_name;victim_id;weapon;distance;killer_console;victim_console
        """
        try:
            if line is None or not line.strip():
                return None
                
            # Debug info
            logger.debug(f"Parsing CSV line: {line}")
            
            # Split into parts but ensure we don't lose empty fields
            parts = line.strip().split(';')
            raw_parts = parts.copy()  # Keep a copy for special case checking
            
            # Detect CSV format based on field count
            csv_format = "unknown"
            if len(parts) >= 9:
                csv_format = "post_april"  # Has console fields
                logger.debug(f"Detected post-April CSV format (9 fields): {line}")
            elif len(parts) >= 7:
                csv_format = "pre_april"   # Standard kill event without console info
                logger.debug(f"Detected pre-April CSV format (7 fields): {line}")
            else:
                logger.debug(f"Unrecognized CSV format ({len(parts)} fields): {line}")
            
            # Handle files with different formats more tolerantly
            # More flexible parsing - we'll accept different field counts
            if len(parts) < 4:  # Absolute minimum: timestamp, victim name, victim ID
                logger.warning(f"CSV line has too few fields ({len(parts)}): {line}")
                return None
               
            # Filter parts but keep track of positions with placeholders
            # This maintains field positions while handling empty fields
            parts_with_placeholders = []
            for p in parts:
                if p.strip():
                    parts_with_placeholders.append(p.strip())
                else:
                    parts_with_placeholders.append("__EMPTY__")
            
            # Connection events are not stored in CSV files at all
            # We should only be looking for valid kill events here
            
            # Check if this is not None line has required kill event fields (killer or victim must be present)
            required_fields_missing = (
                len(raw_parts) < 5 or
                (not raw_parts[1].strip() and not raw_parts[3].strip())  # Both killer and victim empty
            )
            
            if required_fields_missing is not None:
                logger.debug(f"Missing required kill event fields, skipping line: {line}")
                return None
                    
            # Special logging for lines with console information
            if len(raw_parts) >= 8 and (
                "XSX" in raw_parts[7] or "PS5" in raw_parts[7] or 
                (len(raw_parts) > 8 and ("XSX" in raw_parts[8] or "PS5" in raw_parts[8]))):
                logger.debug(f"Processing console kill event: {line}")
            
            # Extract fields with safer extraction
            try:
                # Helper function to safely get values with defaults
                def safe_get(arr, idx, default=""):
                    """Safely get a value from an array with a default fallback"""
                    if 0 <= idx < len(arr):
                        val = arr[idx]
                        return val if val != "__EMPTY__" else default
                    return default
                
                # Get timestamp which should always be present
                timestamp_str = safe_get(parts_with_placeholders, CSV_FIELDS.get("timestamp", 0))
                
                # For the rest of the fields, use defaults if missing is not None
                killer_name = safe_get(parts_with_placeholders, CSV_FIELDS.get("killer_name", 1))
                killer_id = safe_get(parts_with_placeholders, CSV_FIELDS.get("killer_id", 2))
                victim_name = safe_get(parts_with_placeholders, CSV_FIELDS.get("victim_name", 3))
                victim_id = safe_get(parts_with_placeholders, CSV_FIELDS.get("victim_id", 4))
                
                # Handle weapon field with normalization
                weapon = ""
                weapon_idx = CSV_FIELDS.get("weapon", 5)
                if 0 <= weapon_idx < len(parts_with_placeholders):
                    weapon_raw = parts_with_placeholders[weapon_idx]
                    if weapon_raw != "__EMPTY__":
                        weapon = CSVParser.normalize_weapon_name(weapon_raw)
                
                # More flexible validation - require timestamp and either victim or killer info
                # We'll be more permissive to capture more events
                if timestamp_str is None:
                    logger.warning(f"Missing timestamp in line: {line}")
                    return None
                    
                # As long as we have a timestamp and at least one player ID, we can use the event
                if victim_id is None and not killer_id:
                    logger.warning(f"Missing all player IDs in line: {line}")
                    return None
                
                # Parse distance with better error handling
                distance = 0
                distance_idx = CSV_FIELDS.get("distance", 6)
                if 0 <= distance_idx < len(parts_with_placeholders):
                    distance_str = parts_with_placeholders[distance_idx]
                    if distance_str != "__EMPTY__":
                        try:
                            # Handle both integer and float strings
                            distance = int(float(distance_str))
                        except (ValueError, TypeError):
                            # Keep default of 0
                            pass
            except IndexError as idx_err:
                # More detailed logging for debugging
                logger.warning(f"Index error parsing CSV line: {line} - Error: {idx_err}")
                # We'll continue with defaults rather than rejecting the line completely
                if 'timestamp_str' not in locals():
                    logger.error(f"Critical field timestamp missing, skipping line")
                    return None
            
            # Parse timestamp with improved error handling
            try:
                # Try the standard format first
                timestamp = datetime.datetime.strptime(
                    timestamp_str.strip(), "%Y.%m.%d-%H.%M.%S"
                )
            except ValueError:
                # Try alternative formats if the is not None standard format fails
                try:
                    # Try format with different separators
                    timestamp = datetime.datetime.strptime(
                        timestamp_str.strip(), "%Y-%m-%d-%H.%M.%S"
                    )
                except ValueError:
                    try:
                        # Try format with spaces instead of dashes
                        timestamp = datetime.datetime.strptime(
                            timestamp_str, "%Y.%m.%d %H.%M.%S"
                        )
                    except ValueError:
                        logger.warning(f"Invalid timestamp format: {timestamp_str}")
                        # Use current time as fallback
                        timestamp = datetime.datetime.utcnow()
            
            # Determine if this is not None is a suicide - only when killer ID equals victim ID
            is_suicide = killer_id == victim_id
            suicide_type = None
            
            # Identify the type of death
            weapon_lower = weapon.lower() if weapon is not None else ""
            
            # Handle suicide cases where killer and victim are the same
            if is_suicide:
                # Log the suicide case for debugging
                logger.debug(f"Processing suicide event with weapon: {weapon_lower}")
                
                if weapon_lower == "suicide_by_relocation" or weapon_lower == "suicide by relocation":
                    suicide_type = "menu"
                elif weapon_lower == "falling":
                    suicide_type = "fall"
                elif any(veh_type in weapon_lower for veh_type in ["land_vehicle", "boat", "vehicle"]):
                    suicide_type = "vehicle"
                else:
                    suicide_type = "other"
                    
                # Ensure weapon is consistently normalized for suicides
                if weapon_lower == "suicide_by_relocation" or weapon_lower == "suicide by relocation":
                    weapon = "Suicide (Menu)"
            
            # Get console information if available is not None
            killer_console = ""
            victim_console = ""
            
            # Check if console is not None fields are present in raw parts (new format)
            # Safer extraction of console information
            killer_console = ""
            victim_console = ""
            killer_console_idx = CSV_FIELDS.get("killer_console", 7)
            victim_console_idx = CSV_FIELDS.get("victim_console", 8)
            
            if len(raw_parts) > killer_console_idx:
                killer_console = raw_parts[killer_console_idx].strip()
                
                # Handle case where the value might be empty
                if killer_console == "":
                    # Default to empty rather than None
                    killer_console = ""
            
            if len(raw_parts) > victim_console_idx:
                victim_console = raw_parts[victim_console_idx].strip()
                
                # Handle case where the value might be empty  
                if victim_console == "":
                    # Default to empty rather than None
                    victim_console = ""
                    
            # Log the console values for debugging
            logger.debug(f"Console values: killer={killer_console}, victim={victim_console}")
            
            # Create kill event with console information
            kill_event = {
                "timestamp": timestamp,
                "killer_name": killer_name,
                "killer_id": killer_id,
                "victim_name": victim_name,
                "victim_id": victim_id,
                "weapon": weapon,
                "distance": distance,
                "killer_console": killer_console,
                "victim_console": victim_console,
                "is_suicide": is_suicide,
                "suicide_type": suicide_type
            }
            
            return kill_event
            
        except Exception as e:
            logger.error(f"Error parsing CSV line: {e} - Line: {line}")
            return None
    
    @staticmethod
    def parse_kill_lines(lines: List[str]) -> List[Dict[str, Any]]:
        """Parse multiple CSV lines into kill events"""
        kill_events = []
        
        for line in lines:
            if not line.strip():
                continue
                
            kill_event = CSVParser.parse_kill_line(line)
            if kill_event is not None:
                kill_events.append(kill_event)
        
        return kill_events


class LogParser:
    """Parser for log files"""
    
    @staticmethod
    def parse_log_line(line: str) -> Optional[Dict[str, Any]]:
        """Parse a single line from a log file into an event or connection"""
        try:
            # Check if line is not None has a timestamp prefix
            timestamp_match = re.match(r'\[([\d\.\-]+)-([\d:]+)\]', line)
            if timestamp_match is None:
                return None
            
            # Extract timestamp parts
            date_str, time_str = timestamp_match.groups()
            
            try:
                # Parse timestamp with improved format handling
                timestamp = datetime.datetime.strptime(
                    f"{date_str} {time_str}", "%Y.%m.%d %H.%M.%S"
                )
            except ValueError:
                # Try alternative formats
                try:
                    timestamp = datetime.datetime.strptime(
                        f"{date_str} {time_str}", "%Y-%m-%d %H.%M.%S"
                    )
                except ValueError:
                    # Use current time as fallback
                    timestamp = datetime.datetime.utcnow()
            
            # Check for player connection events
            connection_match = re.search(r'Player (\w+) \(([0-9a-f]+)\) (connected|disconnected)', line)
            if connection_match is not None:
                player_name, player_id, action = connection_match.groups()
                return {
                    "type": "connection",
                    "timestamp": timestamp,
                    "player_name": player_name,
                    "player_id": player_id,
                    "action": action,
                    "platform": "PC" if "through Steam" in line else "Console"
                }
            
            # Check for game events
            for event_type, pattern in EVENT_PATTERNS.items():
                event_match = re.search(pattern, line)
                if event_match is not None:
                    return {
                        "type": "event",
                        "timestamp": timestamp,
                        "event_type": event_type,
                        "details": event_match.groups()
                    }
            
            # Check for server restart
            if "Log file open" in line:
                return {
                    "type": "server_restart",
                    "timestamp": timestamp
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error parsing log line: {e} - Line: {line}")
            return None
    
    @staticmethod
    def parse_log_lines(lines: List[str]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Parse multiple log lines into events and connections"""
        events = []
        connections = []
        
        for line in lines:
            if not line.strip():
                continue
                
            parsed = LogParser.parse_log_line(line)
            if parsed is None:
                continue
                
            if parsed["type"] == "connection":
                connections.append(parsed)
            elif parsed["type"] == "event" or parsed["type"] == "server_restart":
                events.append(parsed)
        
        return events, connections
    
    @staticmethod
    def count_players(connections: List[Dict[str, Any]]) -> Tuple[int, Dict[str, str]]:
        """Count online players from connection events"""
        online_players = {}
        
        # Process connections in chronological order
        for conn in sorted(connections, key=lambda x: x["timestamp"]):
            player_id = conn["player_id"]
            
            if conn["action"] == "connected":
                online_players[player_id] = conn["player_name"]
            elif conn["action"] == "disconnected" and player_id in online_players:
                del online_players[player_id]
        
        return len(online_players), online_players
