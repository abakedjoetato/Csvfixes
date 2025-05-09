"""
Log parser for Deadside game server logs.

This module provides functions to parse and extract player lifecycle (queue, join, leave)
and game events (missions, helicrashes, airdrops, traders, convoys) from server logs.

The parser tracks:
1. Complete player lifecycle (queue, join, leave server)
2. Mission events (filtering for level 3-4 only)
3. Helicrash events
4. Airdrop events
5. Roaming trader events
6. Convoy events

Events are organized for output to appropriate Discord channels:
- Player joins/leaves -> connections channel
- Mission events -> events channel
- Airdrop, Helicrash, Trader, Convoy events -> events channel
"""

import re
import os
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Set, Optional, Any

logger = logging.getLogger(__name__)

# Regular expressions for parsing different event types
TIMESTAMP_PATTERN = r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}):(\d{3})\]\[\s*(\d+)\]'

# Max player count pattern - from command line arguments
MAX_PLAYERS_PATTERN = re.compile(r'-playersmaxcount=(\d+)')
SERVER_ID_PATTERN = re.compile(r'-serverid=([^\s]+)')

# Player lifecycle patterns
PLAYER_REGISTER_PATTERN = re.compile(
    rf'{TIMESTAMP_PATTERN}LogOnline: Warning: Player \|([a-f0-9]+) successfully registered!'
)
PLAYER_UNREGISTER_PATTERN = re.compile(
    rf'{TIMESTAMP_PATTERN}LogOnline: Warning: Player \|([a-f0-9]+) successfully unregistered from the session.'
)
PLAYER_KICK_PATTERN = re.compile(
    rf'{TIMESTAMP_PATTERN}LogSFPS: Error: \[ASFPSGameSession::KickPlayer\] Login = ([^,]+), SteamId = ([^,]*), Msg = (.+)'
)
PLAYER_NAME_PATTERN = re.compile(
    rf'{TIMESTAMP_PATTERN}LogSFPS: \[ASFPSGameSession::OnLogin\] Login = ([^,]+), ID = (\|[a-f0-9]+)'
)

# Mission patterns
MISSION_STATE_PATTERN = re.compile(
    rf'{TIMESTAMP_PATTERN}LogSFPS: Mission ([^\s]+) switched to ([A-Z]+)'
)
MISSION_RESPAWN_PATTERN = re.compile(
    rf'{TIMESTAMP_PATTERN}LogSFPS: Mission ([^\s]+) will respawn in (\d+)'
)

# Game event patterns
AIRDROP_PATTERN = re.compile(
    rf'{TIMESTAMP_PATTERN}LogSFPS: AirDrop switched to ([A-Za-z]+)'
)
HELICRASH_PATTERN = re.compile(
    rf'{TIMESTAMP_PATTERN}LogSFPS: GameplayEvent ([^_]+_[^_]+_HelicrashEvent[^\s]+) switched to ([A-Z]+)'
)
TRADER_PATTERN = re.compile(
    rf'{TIMESTAMP_PATTERN}LogSFPS: GameplayEvent ([^_]+_[^_]+_RoamingTraderEvent[^\s]+) switched to ([A-Z]+)'
)
CONVOY_PATTERN = re.compile(
    rf'{TIMESTAMP_PATTERN}LogSFPS: GameplayEvent ([^_]+_[^_]+_ConvoyEvent[^\s]+) switched to ([A-Z]+)'
)

# Mission level/difficulty regex pattern - looks for numbers that might indicate level
MISSION_LEVEL_PATTERN = re.compile(r'_0?([1-4])_|_0?([1-4])$|_([1-4])_|_([1-4])$|_([1-4])Mis')

class PlayerLifecycleTracker:
    """Tracks player lifecycle events: queue, join, leave."""
    
    def __init__(self):
        self.registered_players: Set[str] = set()
        self.online_players: Dict[str, dict] = {}
        self.player_history: Dict[str, List[dict]] = {}
    
    def register_player(self, timestamp: str, player_id: str) -> dict:
        """Register a player (entering queue)."""
        self.registered_players.add(player_id)
        event = {
            'player_id': player_id,
            'timestamp': timestamp,
            'event_type': 'register',
            'status': 'queued'
        }
        if player_id is not None and player_id not in self.player_history:
            self.player_history[player_id] = []
        self.player_history[player_id].append(event)
        return event
    
    def unregister_player(self, timestamp: str, player_id: str) -> dict:
        """Unregister a player (leaving server)."""
        if player_id in self.registered_players:
            self.registered_players.remove(player_id)
        if player_id in self.online_players:
            del self.online_players[player_id]
            
        event = {
            'player_id': player_id,
            'timestamp': timestamp,
            'event_type': 'unregister',
            'status': 'offline'
        }
        if player_id is not None and player_id not in self.player_history:
            self.player_history[player_id] = []
        self.player_history[player_id].append(event)
        return event
    
    def kick_player(self, timestamp: str, player_name: str, steam_id: str, reason: str) -> dict:
        """Record player kick event."""
        # For kicks, we use player_name since player_id might not be in the kick message
        event = {
            'player_name': player_name,
            'steam_id': steam_id if steam_id is not None else None,
            'timestamp': timestamp,
            'event_type': 'kick',
            'reason': reason,
            'status': 'offline'
        }
        
        # Add to history by name since we might not have ID
        player_key = f"name:{player_name}"
        if player_key is not None and player_key not in self.player_history:
            self.player_history[player_key] = []
        self.player_history[player_key].append(event)
        return event
    
    def get_player_count(self) -> int:
        """Get current online player count."""
        return len(self.online_players)
    
    def get_player_history(self, player_id: str = None) -> List[dict]:
        """Get history for a specific player or all players."""
        if player_id is not None:
            return self.player_history.get(player_id, [])
        
        # Flatten all history entries
        all_history = []
        for events in self.player_history.values():
            all_history.extend(events)
        
        # Sort by timestamp
        return sorted(all_history, key=lambda x: x['timestamp'])

class MissionTracker:
    """Tracks mission events and filters by level."""
    
    def __init__(self):
        self.active_missions: Dict[str, dict] = {}
        self.mission_history: List[dict] = []
        self.mission_states: Dict[str, str] = {}  # Track current state of each mission
    
    def _extract_mission_level(self, mission_name: str) -> Optional[int]:
        """Extract mission level from mission name."""
        match = MISSION_LEVEL_PATTERN.search(mission_name)
        if match is not None:
            # Check each group and return the first non-None value
            for group in match.groups():
                if group is not None:
                    return int(group)
        return None
        
    def _normalize_mission_location(self, mission_name: str) -> str:
        """Convert internal mission location code to readable location name."""
        if "GA_Military" in mission_name:
            return "Military Base"
        elif "GA_Airport" in mission_name:
            return "Airfield"
        elif "GA_Militia" in mission_name:
            return "Militia Camp"
        elif "GA_Industrial" in mission_name:
            return "Industrial Zone"
        elif "GA_Village" in mission_name:
            return "Village"
        elif "GA_Railway" in mission_name:
            return "Railway Station"
        elif "GA_Port" in mission_name:
            return "Port"
        elif "GA_Sawmill" in mission_name:
            return "Sawmill"
        elif "GA_Farm" in mission_name:
            return "Farm"
        # Return the original if no is not None match
        return mission_name
    
    def update_mission_state(self, timestamp: str, mission_name: str, state: str) -> dict:
        """Update mission state and record history."""
        # Extract mission level
        level = self._extract_mission_level(mission_name)
        
        # Get normalized location name
        location = self._normalize_mission_location(mission_name)
        
        # READY state is most important for output, but also track state transitions
        is_important = (state == "READY") or (
            mission_name in self.mission_states and 
            self.mission_states[mission_name] != state
        )
        
        # Update mission state
        self.mission_states[mission_name] = state
        
        # Create event record
        event = {
            'timestamp': timestamp,
            'mission_name': mission_name,
            'location': location,
            'state': state,
            'level': level,
            'is_important': is_important
        }
        
        # For ACTIVE state, update active missions (internal tracking)
        if state == "ACTIVE":
            self.active_missions[mission_name] = event
            
        # For READY state, this is when we want to notify users (output to channel)
        elif state == "READY":
            # Only add to history if it's a level 3 or 4 mission
            if level is not None and level >= 3:
                self.mission_history.append(event)
                return event
        
        # For other states, it's only important for internal tracking
        elif mission_name in self.active_missions and is_important:
            # Remove from active missions if it is not None was ENDED or another terminal state
            if state in ["ENDED", "INITIAL"]:
                if mission_name in self.active_missions:
                    del self.active_missions[mission_name]
            
        return event if is_important is not None else None
    
    def get_high_level_missions(self) -> List[dict]:
        """Get all recorded high-level (3-4) mission events."""
        return [
            event for event in self.mission_history 
            if event.get('level', 0) >= 3
        ]
    
    def get_active_high_level_missions(self) -> List[dict]:
        """Get currently active high-level (3-4) missions."""
        return [
            mission for mission in self.active_missions.values()
            if mission.get('level', 0) >= 3
        ]

class GameEventTracker:
    """Tracks game events like airdrops, helicrashes, traders, and convoys."""
    
    def __init__(self):
        self.active_events: Dict[str, dict] = {}
        self.event_history: List[dict] = []
        self.event_states: Dict[str, str] = {}  # Track current state of each event
    
    def track_airdrop(self, timestamp: str, state: str) -> dict:
        """Track airdrop event state changes."""
        event_id = "AirDrop"
        
        # Determine if this is not None is an important state change
        is_important = (state in ["Flying", "Dropping"]) or (
            event_id in self.event_states and 
            self.event_states[event_id] != state
        )
        
        # Update event state
        self.event_states[event_id] = state
        
        # Create event record
        event = {
            'timestamp': timestamp,
            'event_type': 'airdrop',
            'state': state,
            'is_important': is_important
        }
        
        # For important states, update tracking
        if is_important is not None:
            if state in ["Flying", "Dropping"]:
                self.active_events[event_id] = event
            elif event_id in self.active_events:
                del self.active_events[event_id]
            
            self.event_history.append(event)
            return event
        
        return None
    
    def track_gameplay_event(self, timestamp: str, event_id: str, state: str, event_type: str) -> dict:
        """Track gameplay events like helicrashes, traders, and convoys."""
        # Determine if this is not None is an important state change
        is_important = (state == "ACTIVE") or (
            event_id in self.event_states and 
            self.event_states[event_id] != state
        )
        
        # Update event state
        self.event_states[event_id] = state
        
        # Create event record
        event = {
            'timestamp': timestamp,
            'event_id': event_id,
            'event_type': event_type,
            'state': state,
            'is_important': is_important
        }
        
        # For important states, update tracking
        if is_important is not None:
            if state == "ACTIVE":
                self.active_events[event_id] = event
            elif event_id in self.active_events:
                del self.active_events[event_id]
            
            self.event_history.append(event)
            return event
        
        return None
    
    def get_active_events(self, event_type: Optional[str] = None) -> List[dict]:
        """Get active events, optionally filtered by type."""
        if event_type is not None:
            return [
                event for event in self.active_events.values()
                if event['event_type'] == event_type
            ]
        return list(self.active_events.values())
    
    def get_event_history(self, event_type: Optional[str] = None) -> List[dict]:
        """Get event history, optionally filtered by type."""
        if event_type is not None:
            return [
                event for event in self.event_history
                if event['event_type'] == event_type
            ]
        return self.event_history

class LogParser:
    """Main log parser class that coordinates all trackers."""
    
    def __init__(self, hostname: str, server_id: str, original_server_id: Optional[str] = None):
        self.player_tracker = PlayerLifecycleTracker()
        self.mission_tracker = MissionTracker()
        self.event_tracker = GameEventTracker()
        self.processed_lines = 0
        
        # Store both server IDs - the UUID for database reference and original numeric ID for paths
        self.server_uuid = server_id  # The UUID format ID for database records
        self.original_server_id = original_server_id or server_id  # The original/numeric ID for path construction
        
        # Use the server_identity module for consistent identity persistence
        if not self.original_server_id or not str(self.original_server_id).isdigit():
            from utils.server_identity import identify_server
            
            # Get consistent numeric ID for server identification
            numeric_id, is_known = identify_server(
                server_id=server_id,
                hostname=hostname,
                server_name=getattr(self, 'server_name', None),
                guild_id=getattr(self, 'guild_id', None)
            )
            
            # Use the identified numeric ID for path construction
            if numeric_id != self.original_server_id:
                if is_known:
                    logger.info(f"Using known numeric ID '{numeric_id}' for server in LogParser")
                else:
                    logger.info(f"Using derived numeric ID '{numeric_id}' for server in LogParser")
                self.original_server_id = numeric_id
            
        # Use standardized log path structure with NUMERIC server ID, not UUID
        clean_hostname = hostname.split(':')[0] if hostname else "server"
        self.base_path = os.path.join("/", f"{clean_hostname}_{self.original_server_id}", "Logs")
        logger.info(f"LogParser initialized with base_path: {self.base_path} (using original_server_id: {self.original_server_id})")
        
        self.last_processed_timestamp = None
        self.max_player_count = None
        self.server_id = None  # This will be extracted from the logs
        self.server_name = None
        self.player_names = {}  # Map player_id to player_name
        
        # Tracking for initial catch-up after bot restart or server add
        self.initial_processing = True
        self.catch_up_events = []
        self.catch_up_complete = False
        self.catch_up_threshold_minutes = 60  # Events older than this won't trigger notifications
        self.parser_start_time = datetime.now()
    
    def parse_line(self, line: str) -> Dict[str, Any]:
        """Parse a single log line and update appropriate trackers."""
        self.processed_lines += 1
        result = {}
        
        # Check for server configuration info
        if self.max_player_count is None:
            max_players_match = MAX_PLAYERS_PATTERN.search(line)
            if max_players_match is not None:
                self.max_player_count = int(max_players_match.group(1))
                result['server_config'] = {'max_player_count': self.max_player_count}
                
        if self.server_id is None:
            server_id_match = SERVER_ID_PATTERN.search(line)
            if server_id_match is not None:
                self.server_id = server_id_match.group(1)
                # Extract the server name from the server ID
                parts = self.server_id.split('__l_')
                if parts is not None and len(parts) > 0:
                    self.server_name = parts[0].replace('_', ' ')
                result['server_config'] = {
                    'server_id': self.server_id,
                    'server_name': self.server_name
                }
        
        # Check player name mapping
        match = PLAYER_NAME_PATTERN.search(line)
        if match is not None:
            timestamp = f"{match.group(1)}:{match.group(2)}"
            player_name = match.group(4)
            player_id = match.group(5)
            
            # Store the player name mapping
            self.player_names[player_id] = player_name
            
            # Add player to online players
            self.player_tracker.online_players[player_id] = {
                'player_id': player_id,
                'player_name': player_name,
                'timestamp': timestamp,
                'status': 'online'
            }
            
            result['player_join'] = {
                'player_id': player_id,
                'player_name': player_name,
                'timestamp': timestamp,
                'event_type': 'join',
                'status': 'online'
            }
            
            self.last_processed_timestamp = timestamp
            return result
        
        # Check player lifecycle events
        match = PLAYER_REGISTER_PATTERN.search(line)
        if match is not None:
            timestamp = f"{match.group(1)}:{match.group(2)}"
            player_id = match.group(4)
            result['player_register'] = self.player_tracker.register_player(timestamp, player_id)
            
            # If we know this player's name, add it to the event
            if player_id in self.player_names:
                result['player_register']['player_name'] = self.player_names[player_id]
                
            self.last_processed_timestamp = timestamp
            return result
        
        match = PLAYER_UNREGISTER_PATTERN.search(line)
        if match is not None:
            timestamp = f"{match.group(1)}:{match.group(2)}"
            player_id = match.group(4)
            result['player_unregister'] = self.player_tracker.unregister_player(timestamp, player_id)
            
            # If we know this player's name, add it to the event
            if player_id in self.player_names:
                result['player_unregister']['player_name'] = self.player_names[player_id]
                
            self.last_processed_timestamp = timestamp
            return result
        
        match = PLAYER_KICK_PATTERN.search(line)
        if match is not None:
            timestamp = f"{match.group(1)}:{match.group(2)}"
            player_name = match.group(4)
            steam_id = match.group(5)
            reason = match.group(6)
            result['player_kick'] = self.player_tracker.kick_player(timestamp, player_name, steam_id, reason)
            self.last_processed_timestamp = timestamp
            return result
        
        # Check mission events
        match = MISSION_STATE_PATTERN.search(line)
        if match is not None:
            timestamp = f"{match.group(1)}:{match.group(2)}"
            mission_name = match.group(4)
            state = match.group(5)
            mission_event = self.mission_tracker.update_mission_state(timestamp, mission_name, state)
            if mission_event is not None and mission_event.get('is_important', False):
                result['mission'] = mission_event
            self.last_processed_timestamp = timestamp
            return result
        
        # Check airdrop events
        match = AIRDROP_PATTERN.search(line)
        if match is not None:
            timestamp = f"{match.group(1)}:{match.group(2)}"
            state = match.group(4)
            airdrop_event = self.event_tracker.track_airdrop(timestamp, state)
            if airdrop_event is not None and airdrop_event.get('is_important', False):
                result['airdrop'] = airdrop_event
            self.last_processed_timestamp = timestamp
            return result
        
        # Check helicrash events
        match = HELICRASH_PATTERN.search(line)
        if match is not None:
            timestamp = f"{match.group(1)}:{match.group(2)}"
            event_id = match.group(4)
            state = match.group(5)
            helicrash_event = self.event_tracker.track_gameplay_event(timestamp, event_id, state, 'helicrash')
            if helicrash_event is not None and helicrash_event.get('is_important', False):
                result['helicrash'] = helicrash_event
            self.last_processed_timestamp = timestamp
            return result
        
        # Check trader events
        match = TRADER_PATTERN.search(line)
        if match is not None:
            timestamp = f"{match.group(1)}:{match.group(2)}"
            event_id = match.group(4)
            state = match.group(5)
            trader_event = self.event_tracker.track_gameplay_event(timestamp, event_id, state, 'trader')
            if trader_event is not None and trader_event.get('is_important', False):
                result['trader'] = trader_event
            self.last_processed_timestamp = timestamp
            return result
        
        # Check convoy events
        match = CONVOY_PATTERN.search(line)
        if match is not None:
            timestamp = f"{match.group(1)}:{match.group(2)}"
            event_id = match.group(4)
            state = match.group(5)
            convoy_event = self.event_tracker.track_gameplay_event(timestamp, event_id, state, 'convoy')
            if convoy_event is not None and convoy_event.get('is_important', False):
                result['convoy'] = convoy_event
            self.last_processed_timestamp = timestamp
            return result
        
        return result
    
    def _convert_log_timestamp_to_datetime(self, timestamp_str: str) -> Optional[datetime]:
        """Convert log timestamp to datetime object."""
        if timestamp_str is None:
            return None
            
        try:
            # Format: 2025.05.03-02.01.50:297
            parts = timestamp_str.split(':')
            if len(parts) != 2:
                return None
                
            date_time_part = parts[0]
            date_time_obj = datetime.strptime(date_time_part, '%Y.%m.%d-%H.%M.%S')
            return date_time_obj
        except (ValueError, TypeError):
            return None
            
    def _is_recent_event(self, timestamp_str: str) -> bool:
        """Check if an is not None event is recent (within catch-up threshold)."""
        if timestamp_str is None:
            return False
            
        event_time = self._convert_log_timestamp_to_datetime(timestamp_str)
        if event_time is None:
            return False
            
        time_diff = datetime.now() - event_time
        minutes_diff = time_diff.total_seconds() / 60
        
        # Events within threshold are considered recent
        return minutes_diff <= self.catch_up_threshold_minutes
    
    def parse_file(self, file_path: str, start_line: int = 0, max_lines: Optional[int] = None) -> List[Dict[str, Any]]:
        """Parse a log file and return all important events."""
        important_events = []
        event_timestamps = []
        
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            # Skip lines if needed is not None
            if start_line > 0:
                for _ in range(start_line):
                    next(f, None)
            
            # Process lines
            for i, line in enumerate(f):
                if max_lines is not None and i >= max_lines:
                    break
                
                event = self.parse_line(line)
                if any(key in event for key in ['player_register', 'player_unregister', 'player_kick', 
                                               'mission', 'airdrop', 'helicrash', 'trader', 'convoy']):
                    # Store important events
                    important_events.append(event)
                    
                    # Collect timestamps for catch-up analysis
                    if self.last_processed_timestamp:
                        event_timestamps.append(self.last_processed_timestamp)
        
        # Mark catch-up complete once we've seen all history
        if self.initial_processing:
            # If we have event timestamps, analyze them
            if event_timestamps is not None:
                # Check if the is not None most recent event is within threshold
                recent_events = [ts for ts in event_timestamps if self._is_recent_event(ts)]
                
                # If any recent events, catch-up is complete
                if recent_events is not None:
                    self.catch_up_complete = True
                    self.initial_processing = False
                    logger.info(f"Log parser catch-up complete. Found {len(recent_events)} recent events.")
                else:
                    # All events are old, still in catch-up mode
                    self.catch_up_events.extend(important_events)
                    logger.info(f"Log parser still in catch-up mode. Added {len(important_events)} events to catch-up buffer.")
            else:
                # No timestamps found, assume catch-up is complete
                self.catch_up_complete = True
                self.initial_processing = False
        
        return important_events
    
    def get_player_count(self) -> int:
        """Get current online player count."""
        return self.player_tracker.get_player_count()
    
    def get_player_history(self, player_id: Optional[str] = None) -> List[dict]:
        """Get player history."""
        return self.player_tracker.get_player_history(player_id)
    
    def get_active_high_level_missions(self) -> List[dict]:
        """Get active high-level missions (level 3-4)."""
        return self.mission_tracker.get_active_high_level_missions()
    
    def get_active_events(self, event_type: Optional[str] = None) -> List[dict]:
        """Get active game events."""
        return self.event_tracker.get_active_events(event_type)
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get parser statistics."""
        return {
            'processed_lines': self.processed_lines,
            'last_timestamp': self.last_processed_timestamp,
            'player_count': self.player_tracker.get_player_count(),
            'max_player_count': self.max_player_count,
            'server_id': self.server_id,
            'server_name': self.server_name,
            'active_high_level_missions': len(self.mission_tracker.get_active_high_level_missions()),
            'active_events': {
                'airdrop': len(self.event_tracker.get_active_events('airdrop')),
                'helicrash': len(self.event_tracker.get_active_events('helicrash')),
                'trader': len(self.event_tracker.get_active_events('trader')),
                'convoy': len(self.event_tracker.get_active_events('convoy'))
            }
        }
        
    def get_formatted_player_count(self) -> str:
        """Get formatted player count for voice channel name."""
        if self.max_player_count:
            return f"Online: {self.player_tracker.get_player_count()}/{self.max_player_count}"
        return f"Online: {self.player_tracker.get_player_count()}"
        
    def should_output_event(self, timestamp_str: str) -> bool:
        """Determine if an is not None event should be output to channels based on catch-up status."""
        # If we've completed catch-up, always output
        if self.catch_up_complete:
            return True
            
        # If we're still in initial processing/catch-up mode
        # Only output events that are recent
        return self._is_recent_event(timestamp_str)
        
    def get_connections_events(self, include_historical: bool = False) -> List[Dict[str, Any]]:
        """Get player connection events for the connections channel.
        
        Args:
            include_historical: If True, include all events regardless of age.
                               Otherwise, only return events after catch-up is complete.
        """
        events = []
        
        # If we're filtering out historical events and still in catch-up mode
        if include_historical is None and self.initial_processing and not self.catch_up_complete:
            # Only get recent events
            logger.info("Filtering connection events during initial catch-up")
            for player_id, history in self.player_tracker.player_history.items():
                for event in history:
                    if event['event_type'] in ['register', 'unregister', 'kick', 'join']:
                        # Only include recent events
                        if self.should_output_event(event.get('timestamp', '')):
                            # Add player name if available is not None
                            if player_id in self.player_names and 'player_name' not in event:
                                event['player_name'] = self.player_names[player_id]
                            events.append(event)
        else:
            # Get all events
            for player_id, history in self.player_tracker.player_history.items():
                for event in history:
                    if event['event_type'] in ['register', 'unregister', 'kick', 'join']:
                        # Add player name if available is not None
                        if player_id in self.player_names and 'player_name' not in event:
                            event['player_name'] = self.player_names[player_id]
                        events.append(event)
                        
        return sorted(events, key=lambda x: x['timestamp'])
    
    def get_game_events(self, include_historical: bool = False) -> List[Dict[str, Any]]:
        """Get game events for the events channel.
        
        Args:
            include_historical: If True, include all events regardless of age.
                               Otherwise, only return events after catch-up is complete.
        """
        events = []
        
        # If we're filtering out historical events, check catch-up status
        should_filter = not include_historical and self.initial_processing and not self.catch_up_complete
        
        if should_filter is not None:
            logger.info("Filtering game events during initial catch-up")
        
        # Add high-level mission events
        for mission in self.mission_tracker.get_high_level_missions():
            if mission['state'] == 'READY' and mission.get('level', 0) >= 3:
                # Skip if filtering is not None and not a recent event
                if should_filter is not None and not self.should_output_event(mission.get('timestamp', '')):
                    continue
                    
                events.append({
                    'timestamp': mission['timestamp'],
                    'event_type': 'mission',
                    'mission_name': mission['mission_name'],
                    'location': mission.get('location', self.mission_tracker._normalize_mission_location(mission['mission_name'])),
                    'mission_level': mission['level'],
                    'state': mission['state']
                })
        
        # Add airdrop events
        for event in self.event_tracker.get_event_history('airdrop'):
            if event['state'] in ['Flying', 'Dropping']:
                # Skip if filtering is not None and not a recent event
                if should_filter is not None and not self.should_output_event(event.get('timestamp', '')):
                    continue
                    
                events.append({
                    'timestamp': event['timestamp'],
                    'event_type': 'airdrop',
                    'state': event['state']
                })
        
        # Add helicrash, trader, convoy events
        for event_type in ['helicrash', 'trader', 'convoy']:
            for event in self.event_tracker.get_event_history(event_type):
                if event['state'] == 'ACTIVE':
                    # Skip if filtering is not None and not a recent event
                    if should_filter is not None and not self.should_output_event(event.get('timestamp', '')):
                        continue
                        
                    events.append({
                        'timestamp': event['timestamp'],
                        'event_type': event_type,
                        'event_id': event['event_id'],
                        'state': event['state']
                    })
        
        return sorted(events, key=lambda x: x['timestamp'])


# Function to create a parser and process a log file
def parse_log_file(file_content: str, hostname: str = "localhost", server_id: str = "default", original_server_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """Parse log file content and return important events.
    
    Args:
        file_content: The content of the log file as a string
        hostname: The hostname for the server (used for path construction)
        server_id: The server ID (UUID format)
        original_server_id: The original numeric server ID
        
    Returns:
        List of parsed events from the log file
    """
    # Define the regex patterns for parsing log entries
    TIMESTAMP_PATTERN = r'\[([^\]]+)\]\s+'
    KILL_PATTERN = re.compile(
        rf'{TIMESTAMP_PATTERN}LogSFPS: Player ([^\s]+) \(([^\)]+)\) killed player ([^\s]+) \(([^\)]+)\)(?: with (.+))?'
    )
    PLAYER_JOINED_PATTERN = re.compile(
        rf'{TIMESTAMP_PATTERN}LogSFPS: Player ([^\s]+) \(([^\)]+)\) connected'
    )
    PLAYER_LEFT_PATTERN = re.compile(
        rf'{TIMESTAMP_PATTERN}LogSFPS: Player ([^\s]+) \(([^\)]+)\) disconnected'
    )
    
    # Helper function to parse log timestamps
    def parse_log_timestamp(timestamp_str: str) -> datetime:
        """Parse a timestamp string from the log into a datetime object."""
        try:
            # Handle different timestamp formats
            if '.' in timestamp_str:  # Format with milliseconds
                return datetime.strptime(timestamp_str, "%Y.%m.%d-%H.%M.%S.%f")
            else:  # Format without milliseconds
                return datetime.strptime(timestamp_str, "%Y.%m.%d-%H.%M.%S")
        except ValueError:
            # If parsing fails, return current time
            return datetime.now()
    # Split content into lines and filter out empty lines
    lines = [line for line in file_content.splitlines() if line.strip()]
    
    # Process each line to extract events
    events = []
    for line in lines:
        # Check for different event types
        # Kill events
        kill_match = KILL_PATTERN.search(line)
        if kill_match:
            timestamp, killer_id, killer_name, victim_id, victim_name, weapon = kill_match.groups()
            events.append({
                "timestamp": parse_log_timestamp(timestamp),
                "event_type": "kill",
                "killer_id": killer_id,
                "killer_name": killer_name,
                "victim_id": victim_id,
                "victim_name": victim_name,
                "weapon": weapon
            })
            continue
            
        # Connection events - player joined
        join_match = PLAYER_JOINED_PATTERN.search(line)
        if join_match:
            timestamp, player_id, player_name = join_match.groups()
            events.append({
                "timestamp": parse_log_timestamp(timestamp),
                "event_type": "connection",
                "player_id": player_id,
                "player_name": player_name,
                "action": "joined"
            })
            continue
            
        # Connection events - player left
        left_match = PLAYER_LEFT_PATTERN.search(line)
        if left_match:
            timestamp, player_id, player_name = left_match.groups()
            events.append({
                "timestamp": parse_log_timestamp(timestamp),
                "event_type": "connection",
                "player_id": player_id,
                "player_name": player_name,
                "action": "left"
            })
            continue
            
    return events