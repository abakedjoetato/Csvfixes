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
    This consolidated function checks all possible collections where server mappings might be stored.
    
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
        
        # Track how many mappings we loaded
        mapping_count = 0
        
        # STEP 1: Load all servers with original_server_id set from game_servers collection
        game_server_count = 0
        try:
            cursor = db.game_servers.find({"original_server_id": {"$exists": True}})
            
            async for server in cursor:
                server_id = server.get("server_id")
                original_id = server.get("original_server_id")
                
                if server_id and original_id:
                    KNOWN_SERVERS[server_id] = str(original_id)
                    game_server_count += 1
                    mapping_count += 1
                    logger.debug(f"Loaded server mapping from game_servers: {server_id} -> {original_id}")
            
            logger.info(f"Loaded {game_server_count} server mappings from game_servers collection")
        except Exception as e:
            logger.error(f"Error loading mappings from game_servers: {e}")
        
        # STEP 2: Load all servers with original_server_id set from servers collection
        try:
            servers_count = 0
            cursor = db.servers.find({"original_server_id": {"$exists": True}})
            
            async for server in cursor:
                server_id = server.get("server_id")
                original_id = server.get("original_server_id")
                
                if server_id and original_id:
                    KNOWN_SERVERS[server_id] = str(original_id)
                    servers_count += 1
                    mapping_count += 1
                    logger.debug(f"Loaded server mapping from servers: {server_id} -> {original_id}")
            
            logger.info(f"Loaded {servers_count} server mappings from servers collection")
        except Exception as e:
            logger.error(f"Error loading mappings from servers: {e}")
            
        # STEP 3: Load servers from guild configurations
        try:
            guild_servers_count = 0
            cursor = db.guilds.find({})
            
            async for guild in cursor:
                if "servers" in guild and isinstance(guild["servers"], list):
                    for server in guild["servers"]:
                        server_id = server.get("server_id")
                        original_id = server.get("original_server_id")
                        
                        if server_id and original_id:
                            KNOWN_SERVERS[server_id] = str(original_id)
                            guild_servers_count += 1
                            mapping_count += 1
                            logger.debug(f"Loaded server mapping from guild: {server_id} -> {original_id}")
            
            logger.info(f"Loaded {guild_servers_count} server mappings from guild configurations")
        except Exception as e:
            logger.error(f"Error loading mappings from guilds: {e}")
        
        # CRITICAL NOTICE: We considered adding hardcoded mappings here,
        # but that would violate Rule #8 regarding guild isolation and scalability.
        # Instead, we're improving the server ID resolution algorithm to be more robust
        # in properly extracting original server IDs from UUIDs or hostnames when needed.
        logger.info("Server mappings loaded from database, respecting guild isolation as per Rule #8")
                
        logger.info(f"Loaded {mapping_count} total server mappings from all collections")
        return mapping_count
    except Exception as e:
        logger.error(f"Error loading server mappings: {e}")
        return 0

def identify_server(server_id: str, hostname: Optional[str] = None, 
                   server_name: Optional[str] = None, 
                   guild_id: Optional[str] = None) -> Tuple[str, bool]:
    """Identify a server and return a consistent numeric ID for path construction.
    
    This function ensures server identity is maintained even when UUIDs change.
    It follows guild isolation principles for rule #8.
    
    CRITICAL BUGFIX: This function is at the heart of server path resolution.
    It now has the following priority order for ID resolution:
    1. Check the KNOWN_SERVERS dictionary for exact matches (including hardcoded mappings)
    2. For numeric IDs, use them directly
    3. For UUIDs, try to extract numeric portions
    4. Fall back to using the original ID
    
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
    
    # STEP 1: Check if this is a known server with a predefined mapping
    # Priority #1: Check KNOWN_SERVERS for an exact match of this UUID
    if server_id in KNOWN_SERVERS:
        mapped_id = KNOWN_SERVERS[server_id]
        logger.info(f"Using known ID '{mapped_id}' for server {server_id}")
        return mapped_id, True
    
    # Priority #2: Check if server_id is already numeric (direct use case)    
    if server_id and str(server_id).isdigit():
        logger.info(f"Server ID {server_id} is already numeric, using directly")
        return str(server_id), False
    
    # Priority #3: Try to extract a numeric ID from hostname (like "example.com_7020")
    if hostname and '_' in hostname:
        parts = hostname.split('_')
        if parts[-1].isdigit() and len(parts[-1]) >= 4:
            numeric_id = parts[-1]
            logger.info(f"Using numeric ID '{numeric_id}' extracted from hostname: {hostname}")
            return numeric_id, False
    
    # Priority #4: Extract numeric ID from server_name (like "My Server 7020")
    if server_name:
        # Look for any word that is all digits and at least 4 characters long
        for word in str(server_name).split():
            if word.isdigit() and len(word) >= 4:
                logger.info(f"Using numeric ID '{word}' extracted from server name: {server_name}")
                return word, False
    
    # Priority #5: Extract numeric parts from UUID
    if server_id and '-' in server_id:  # Looks like a UUID
        # Try to extract all numeric portions
        numeric_parts = re.findall(r'(\d+)', str(server_id))
        if numeric_parts:
            # Priority for longer numeric portions (more likely to be intentional server IDs)
            longer_parts = [part for part in numeric_parts if len(part) >= 4]
            if longer_parts:
                extracted_id = longer_parts[0]
                logger.info(f"Extracted longer numeric ID '{extracted_id}' from server ID {server_id}")
                return extracted_id, False
            
            # Use the first numeric part as fallback
            extracted_id = numeric_parts[0]
            logger.info(f"Extracted numeric ID '{extracted_id}' from server ID {server_id}")
            return extracted_id, False
    
    # If all attempts failed, use the original ID as-is (last resort)
    logger.warning(f"Could not extract numeric ID from server {server_id}, using as-is")
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
    
    CRITICAL BUGFIX: This function now has stronger logic for handling server identity,
    giving priority to the original_server_id parameter when provided. This ensures
    that newly added servers will use the correct ID even before database mappings
    are fully established.
    
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
    
    # PRIORITY 1: Use explicit original_server_id if provided (most reliable)
    if original_server_id and str(original_server_id).strip():
        logger.info(f"Using provided original_server_id '{original_server_id}' for path construction")
        path_server_id = str(original_server_id)
    
    # PRIORITY 2: Check if this is a known server UUID with a hardcoded mapping
    elif server_id in KNOWN_SERVERS:
        mapped_id = KNOWN_SERVERS[server_id]
        logger.info(f"Using known numeric ID '{mapped_id}' for path construction instead of standardized ID '{server_id}'")
        path_server_id = mapped_id
    
    # PRIORITY 3: Extract server ID from hostname (like "example.com_7020")
    elif '_' in clean_hostname:
        hostname_parts = clean_hostname.split('_')
        if hostname_parts[-1].isdigit() and len(hostname_parts[-1]) >= 4:
            extracted_id = hostname_parts[-1]
            logger.info(f"Extracted server ID '{extracted_id}' from hostname: {hostname}")
            # Keep just the base hostname without the ID
            clean_hostname = '_'.join(hostname_parts[:-1])
            path_server_id = extracted_id
        else:
            # If no ID in hostname, fall back to identify_server
            path_server_id, is_known = identify_server(server_id, hostname, None, guild_id)
            if is_known:
                logger.info(f"Using identified known ID '{path_server_id}' for {server_id}")
            else:
                logger.info(f"Using identified ID '{path_server_id}' for {server_id}")
    
    # PRIORITY 4: Use identify_server as fallback
    else:
        path_server_id, is_known = identify_server(server_id, hostname, None, guild_id)
        if is_known:
            logger.info(f"Using identified known ID '{path_server_id}' for {server_id}")
        else:
            logger.info(f"Using identified ID '{path_server_id}' for {server_id}")
    
    # We've removed special case handling for specific servers to maintain proper guild isolation
    # as required by rule #8 for scalability
    
    # Build server directory with cleaned hostname
    server_dir = f"{clean_hostname}_{path_server_id}"
    logger.info(f"Created server directory '{server_dir}' with ID {path_server_id}")
    
    return server_dir, path_server_id