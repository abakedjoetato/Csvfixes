"""
Server Identity utility for the Emeralds Killfeed PvP Statistics Discord Bot.

This module maintains server identity across UUID changes and ensures 
proper guild isolation for server identifiers.
"""

import re
import os
import logging
from typing import Dict, Optional, Tuple, Any, List, Union

logger = logging.getLogger(__name__)

# Dictionary to store server UUID to numeric ID mappings
# This will be populated from the database at runtime
KNOWN_SERVERS = {}

async def load_server_mappings(db):
    """
    Load server mappings from the database to populate KNOWN_SERVERS dictionary.
    
    Args:
        db: Database connection object
        
    Returns:
        Number of mappings loaded
    """
    global KNOWN_SERVERS
    
    if db is None:
        logger.warning("Cannot load server mappings: database connection is None")
        return 0
        
    try:
        # Clear existing mappings to prevent stale data
        KNOWN_SERVERS.clear()
        
        # Load all servers with original_server_id set
        cursor = db.game_servers.find({"original_server_id": {"$exists": True}})
        count = 0
        
        async for server in cursor:
            server_id = server.get("server_id")
            original_id = server.get("original_server_id")
            
            if server_id and original_id:
                KNOWN_SERVERS[server_id] = str(original_id)
                count += 1
                logger.debug(f"Loaded server mapping: {server_id} -> {original_id}")
                
        logger.info(f"Loaded {count} server mappings from database")
        return count
    except Exception as e:
        logger.error(f"Error loading server mappings: {e}")
        return 0

def identify_server(server_id: str, hostname: Optional[str] = None, 
                   server_name: Optional[str] = None, 
                   guild_id: Optional[str] = None) -> Tuple[str, bool]:
    """Identify a server and return a consistent numeric ID for path construction.
    
    This function ensures server identity is maintained even when UUIDs change.
    It follows guild isolation principles for rule #8.
    
    Args:
        server_id: The server ID (usually UUID) from the database
        hostname: Optional server hostname
        server_name: Optional server name
        guild_id: Optional Discord guild ID for isolation
        
    Returns:
        Tuple of (numeric_id, is_known_server)
        - numeric_id: Stable numeric ID for path construction
        - is_known_server: Whether this is a known server with predefined ID
    """
    # Ensure we're working with strings
    server_id = str(server_id) if server_id is not None else ""
    hostname = str(hostname) if hostname is not None else ""
    server_name = str(server_name) if server_name is not None else ""
    guild_id = str(guild_id) if guild_id is not None else ""
    # Special case handling for known servers
    if server_id in KNOWN_SERVERS:
        logger.info(f"Using known ID '{KNOWN_SERVERS[server_id]}' for server {server_id}")
        return KNOWN_SERVERS[server_id], True
        
    # No hardcoded server detection - rely on database for server identity
        
    # Try to extract numeric part from server ID if it exists
    if server_id:
        # If server_id is already numeric, use it
        if str(server_id).isdigit():
            return str(server_id), False
            
        # Try to extract a numeric portion from the UUID
        numeric_part = re.search(r'(\d+)', str(server_id))
        if numeric_part:
            extracted_id = numeric_part.group(1)
            # Look for longer numeric IDs first (more likely to be intentional server IDs)
            longer_parts = [part for part in re.findall(r'(\d+)', str(server_id)) if len(part) >= 4]
            if longer_parts:
                extracted_id = longer_parts[0]
                
            logger.info(f"Extracted numeric ID '{extracted_id}' from server ID {server_id}")
            return extracted_id, False
    
    # If no numeric part, return the original ID (which will be used as-is)
    return str(server_id), False

async def resolve_server_id(db, server_id: str, guild_id: Optional[str] = None) -> Dict[str, Any]:
    """Comprehensively resolve a server ID to find the server configuration.
    
    This function searches all collections using various ID forms (UUID, numeric ID, original_server_id)
    to ensure consistent server identity resolution across the application.
    
    Args:
        db: Database connection
        server_id: The server ID (UUID or numeric ID) to resolve
        guild_id: Optional Discord guild ID for isolation
        
    Returns:
        Dict containing server configuration or empty dict if not found:
        - "server_id": The standardized server ID (UUID)
        - "original_server_id": The original server ID (numeric)
        - "config": The complete server configuration
        - "collection": The collection where the server was found
    """
    if not db or not server_id:
        logger.warning(f"Cannot resolve server ID: {'db is None' if not db else f'invalid server_id: {server_id}'}")
        return {}
        
    # Ensure we're working with strings
    server_id = str(server_id) if server_id else ""
    guild_id = str(guild_id) if guild_id else ""
    
    logger.info(f"Resolving server ID: {server_id} (guild: {guild_id or 'None'})")
    
    # First, handle the case where server_id is a numeric ID that might be an original_server_id
    original_id_match = None
    if server_id.isdigit():
        logger.info(f"Server ID {server_id} is numeric, checking if it matches any original_server_id")
        # Look for servers with this as original_server_id
        try:
            # Search in game_servers collection
            server = await db.game_servers.find_one({"original_server_id": server_id})
            if server:
                logger.info(f"Found server with original_server_id={server_id} in game_servers: {server.get('server_id')}")
                return {
                    "server_id": server.get("server_id"),
                    "original_server_id": server_id,
                    "config": server,
                    "collection": "game_servers"
                }
                
            # Also search in servers collection
            server = await db.servers.find_one({"original_server_id": server_id})
            if server:
                logger.info(f"Found server with original_server_id={server_id} in servers: {server.get('server_id')}")
                return {
                    "server_id": server.get("server_id"),
                    "original_server_id": server_id,
                    "config": server,
                    "collection": "servers"
                }
                
            # If guild_id provided, also search in that guild's servers
            if guild_id:
                guild = await db.guilds.find_one({"guild_id": guild_id})
                if guild and "servers" in guild:
                    for guild_server in guild.get("servers", []):
                        if str(guild_server.get("original_server_id")) == server_id:
                            logger.info(f"Found server with original_server_id={server_id} in guild {guild_id}")
                            return {
                                "server_id": guild_server.get("server_id"),
                                "original_server_id": server_id,
                                "config": guild_server,
                                "collection": "guilds.servers"
                            }
        except Exception as e:
            logger.error(f"Error searching for server by original_server_id={server_id}: {e}")
    
    # Next, try direct lookup by server_id
    try:
        # Try game_servers first
        server = await db.game_servers.find_one({"server_id": server_id})
        if server:
            logger.info(f"Found server with server_id={server_id} in game_servers")
            return {
                "server_id": server_id,
                "original_server_id": server.get("original_server_id"),
                "config": server,
                "collection": "game_servers"
            }
            
        # Then try servers collection
        server = await db.servers.find_one({"server_id": server_id})
        if server:
            logger.info(f"Found server with server_id={server_id} in servers")
            return {
                "server_id": server_id,
                "original_server_id": server.get("original_server_id"),
                "config": server,
                "collection": "servers"
            }
            
        # If guild_id provided, also check that guild's servers
        if guild_id:
            guild = await db.guilds.find_one({"guild_id": guild_id})
            if guild and "servers" in guild:
                for guild_server in guild.get("servers", []):
                    if guild_server.get("server_id") == server_id:
                        logger.info(f"Found server with server_id={server_id} in guild {guild_id}")
                        return {
                            "server_id": server_id,
                            "original_server_id": guild_server.get("original_server_id"),
                            "config": guild_server,
                            "collection": "guilds.servers"
                        }
    except Exception as e:
        logger.error(f"Error searching for server by server_id={server_id}: {e}")
        
    # Server not found after checking all sources
    logger.warning(f"Server with ID {server_id} not found in any collection")
    return {}

def get_path_components(server_id: str, hostname: str, 
                       original_server_id: Optional[str] = None,
                       guild_id: Optional[str] = None) -> Tuple[str, str]:
    """Get path components for server directories.
    
    This builds the directory paths consistently with server identity.
    
    Args:
        server_id: The server ID (usually UUID) from the database
        hostname: Server hostname
        original_server_id: Optional original server ID to override detection
        guild_id: Optional Discord guild ID for isolation
        
    Returns:
        Tuple of (server_dir, path_server_id)
        - server_dir: The server directory name (hostname_serverid)
        - path_server_id: The server ID to use in paths
    """
    # Ensure we're working with strings
    server_id = str(server_id) if server_id is not None else ""
    hostname = str(hostname) if hostname is not None else ""
    original_server_id = str(original_server_id) if original_server_id is not None else ""
    guild_id = str(guild_id) if guild_id is not None else ""
    # Clean hostname - handle both port specifications (:22) and embedded IDs (_1234)
    clean_hostname = hostname.split(':')[0] if hostname else "server"
    
    # If hostname already contains a numeric ID at the end, extract it
    hostname_has_id = False
    if '_' in clean_hostname:
        hostname_parts = clean_hostname.split('_')
        if hostname_parts[-1].isdigit():
            # Hostname already contains ID (like "example.com_1234")
            clean_hostname = '_'.join(hostname_parts[:-1])  # Remove ID portion
            hostname_has_id = True
    
    # Get path server ID (using original_server_id if provided)
    if original_server_id and str(original_server_id).strip():
        path_server_id = str(original_server_id)
    else:
        path_server_id, _ = identify_server(server_id, hostname, None, guild_id)
    
    # Build server directory with cleaned hostname
    server_dir = f"{clean_hostname}_{path_server_id}"
    
    return server_dir, path_server_id