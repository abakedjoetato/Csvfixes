
"""Path utilities for standardized file path handling across parsers"""

import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)

def clean_hostname(hostname: Optional[str]) -> str:
    """Remove port from hostname if present"""
    if not hostname:
        return "server"
    return hostname.split(':')[0]

def get_base_path(hostname: str, server_id: str, original_server_id: Optional[str] = None) -> str:
    """Get standardized base path for server
    
    Args:
        hostname: Server hostname
        server_id: Standardized server ID
        original_server_id: Original, unstandardized server ID for path construction
    
    Returns:
        Base path for server files
    """
    clean_host = clean_hostname(hostname)
    # Use original_server_id for path construction if provided
    path_server_id = original_server_id if original_server_id else server_id
    
    if original_server_id and original_server_id != server_id:
        logger.debug(f"Using original server ID '{original_server_id}' for path construction instead of '{server_id}'")
    
    return os.path.join("/", f"{clean_host}_{path_server_id}")

def get_log_path(hostname: str, server_id: str, original_server_id: Optional[str] = None) -> str:
    """Get standardized log file path
    
    Args:
        hostname: Server hostname
        server_id: Standardized server ID
        original_server_id: Original, unstandardized server ID for path construction
    
    Returns:
        Path to log directory
    """
    return os.path.join(get_base_path(hostname, server_id, original_server_id), "Logs")

def get_csv_path(hostname: str, server_id: str, world_dir: Optional[str] = None, original_server_id: Optional[str] = None) -> str:
    """Get standardized CSV file path
    
    Args:
        hostname: Server hostname
        server_id: Standardized server ID
        world_dir: Optional world directory name (e.g. 'world_0')
        original_server_id: Original, unstandardized server ID for path construction
    
    Returns:
        Path to CSV files directory
    """
    # Clean hostname and use original_server_id for consistency
    clean_host = clean_hostname(hostname)
    path_server_id = original_server_id if original_server_id else server_id
    
    # Always start from root with hostname_serverid structure
    server_dir = f"{clean_host}_{path_server_id}"
    deathlogs_base = os.path.join("/", server_dir, "actual1", "deathlogs")
    
    if not world_dir:
        return deathlogs_base
        
    # Add world directory if specified, ensuring clean path joining
    return os.path.normpath(os.path.join(deathlogs_base, world_dir))

def get_log_file_path(hostname: str, server_id: str, original_server_id: Optional[str] = None) -> str:
    """Get full path to Deadside.log
    
    Args:
        hostname: Server hostname
        server_id: Standardized server ID
        original_server_id: Original, unstandardized server ID for path construction
    
    Returns:
        Full path to Deadside.log file
    """
    # Always use Logs path for log files
    return os.path.join(get_log_path(hostname, server_id, original_server_id), "Deadside.log")
