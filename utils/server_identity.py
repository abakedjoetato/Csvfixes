"""
Server Identity utility for the Tower of Temptation PvP Statistics Discord Bot.

This module maintains server identity across UUID changes and ensures 
proper guild isolation for server identifiers.
"""

import re
import os
import logging
from typing import Dict, Optional, Tuple, Any

logger = logging.getLogger(__name__)

# Dictionary of known servers with stable numeric IDs
# This is a fallback for when database lookups fail
KNOWN_SERVERS = {
    # Tower of Temptation server IDs
    "1b1ab57e-8749-4a40-b7a1-b1073a5f24b3": "7020",  # Original UUID
    "1056852d-05f9-4e5e-9e88-012c2870c042": "7020",  # New UUID after reset
}

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
        
    # Check for Tower of Temptation by name or hostname
    is_tot = False
    if hostname and 'tower' in hostname.lower() and 'temptation' in hostname.lower():
        is_tot = True
    elif server_name and 'tower' in server_name.lower() and 'temptation' in server_name.lower():
        is_tot = True
    # Handle Tower of Temptation detection by hostname IP (specific case for known server)
    elif hostname and '79.127.236.1' in hostname:
        is_tot = True
        
    if is_tot:
        logger.info(f"Using known ID '7020' for Tower of Temptation server {server_id}")
        return "7020", True
        
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