
"""Path utilities for standardized file path handling across parsers"""

import os
from typing import Optional

def clean_hostname(hostname: Optional[str]) -> str:
    """Remove port from hostname if present"""
    if not hostname:
        return "server"
    return hostname.split(':')[0]

def get_base_path(hostname: str, server_id: str) -> str:
    """Get standardized base path for server"""
    clean_host = clean_hostname(hostname)
    return os.path.join("/", f"{clean_host}_{server_id}")

def get_log_path(hostname: str, server_id: str) -> str:
    """Get standardized log file path"""
    return os.path.join(get_base_path(hostname, server_id), "Logs")

def get_csv_path(hostname: str, server_id: str, world_dir: Optional[str] = None) -> str:
    """Get standardized CSV file path
    
    Args:
        hostname: Server hostname
        server_id: Server ID
        world_dir: Optional world directory name (e.g. 'world_0')
    """
    # Always use actual1/deathlogs path structure for CSV files
    base_path = os.path.join(get_base_path(hostname, server_id), "actual1", "deathlogs")
    if world_dir:
        return os.path.join(base_path, world_dir)
    return base_path

def get_log_file_path(hostname: str, server_id: str) -> str:
    """Get full path to Deadside.log"""
    # Always use Logs path for log files
    return os.path.join(get_base_path(hostname, server_id), "Logs", "Deadside.log")

def get_log_file_path(hostname: str, server_id: str) -> str:
    """Get full path to Deadside.log"""
    return os.path.join(get_log_path(hostname, server_id), "Deadside.log")
