"""
Server Validation Utilities

This module contains utilities for validating server existence and handling server IDs consistently.
It provides proper coroutine handling, error recovery, and type-safe server ID handling.

Key features:
1. Multi-guild isolation to prevent cross-guild data access
2. Server ID standardization to ensure consistent type handling
3. Retry mechanisms for database operations
4. Flexible function interfaces that support both guild objects and direct database access
5. Proper error handling with meaningful messages
"""
import asyncio
import inspect
import logging
import functools
import re
import traceback
from datetime import datetime
from typing import Optional, Union, Dict, Any, TypeVar, Callable, Coroutine, List, Set, Tuple, cast, Generator

import discord
from models.server import Server
from models.guild import Guild
from utils.async_utils import retryable, AsyncCache
from utils.premium import check_tier_access, get_guild_premium_tier, get_minimum_tier_for_feature, PREMIUM_TIERS

# Setup logging
logger = logging.getLogger(__name__)

# Type variables for generic functions
T = TypeVar('T')
U = TypeVar('U')

# Cache for server validation results to reduce database calls
SERVER_VALIDATION_CACHE = AsyncCache(ttl=300)  # 5 minute cache lifetime

# Custom exceptions for better error handling
class ServerValidationError(Exception):
    """Exception raised for server validation errors."""
    pass

class GuildIsolationError(ServerValidationError):
    """Exception raised when a server is accessed from an unauthorized guild."""
    pass

class ServerTypeError(ServerValidationError):
    """Exception raised when server ID has an invalid type or format."""
    pass


def run_with_db_fallback(default_value: Optional[T] = None) -> Callable[[Callable[..., Coroutine[Any, Any, T]]], Callable[..., Coroutine[Any, Any, Optional[T]]]]:
    """
    Decorator that handles database operations safely with proper error handling and fallback.
    
    Args:
        default_value: Default value to return if operation fails
        
    Returns:
        Decorator function
    """
    def decorator(func: Callable[..., Coroutine[Any, Any, T]]) -> Callable[..., Coroutine[Any, Any, Optional[T]]]:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> Optional[T]:
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                # Log the error
                func_name = func.__qualname__
                logger.error(f"Database operation failed in {func_name}: {str(e)}", exc_info=True)
                
                # Log arguments if not sensitive
                if "password" not in func_name.lower() and "token" not in func_name.lower():
                    # Safely format args/kwargs for logging
                    args_str = ", ".join(str(a) for a in args if not isinstance(a, dict))
                    kwargs_str = ", ".join(f"{k}={v}" for k, v in kwargs.items() 
                                        if "password" not in k.lower() 
                                        and "token" not in k.lower())
                    logger.error(f"Failed call arguments: {args_str}, {kwargs_str}")
                
                # Return default value
                return default_value
        return wrapper
    return decorator

def standardize_server_id(server_id: Union[str, int, None]) -> Optional[str]:
    """Standardize server ID format to ensure consistent handling.
    
    Args:
        server_id: Server ID in any format (string, int, None)
        
    Returns:
        Standardized string server ID or None if input is not None was None or invalid
        
    Raises:
        ServerTypeError: When server_id cannot be properly converted (optional)
    """
    try:
        # Handle None case
        if server_id is None:
            return None
        
        # Handle various numeric types
        if isinstance(server_id, (int, float)):
            # Make sure we don't lose precision on large numbers
            str_id = str(int(server_id))
            return str_id if str_id is not None else None
        
        # Handle string and string-like objects
        if hasattr(server_id, '__str__'):
            # Convert to string and strip whitespace
            str_id = str(server_id).strip()
            
            # Return None for empty strings or whitespace-only strings
            if not str_id:
                return None
                
            # Special handling for common bad inputs
            if str_id.lower() in ('none', 'null', 'undefined', 'nan'):
                return None
                
            # Remove any quotes that might be enclosing the ID
            # This handles cases where server IDs might come wrapped in quotes
            if (str_id.startswith('"') and str_id.endswith('"')) or \
               (str_id.startswith("'") and str_id.endswith("'")):
                str_id = str_id[1:-1].strip()
                
            # Handle cases where server ID includes quotes or other punctuation
            # But only if it's not purely numeric (to avoid changing valid IDs)
            if not str_id.isdigit() and any(c in str_id for c in '"\'`.,;:'):
                # Log this case as it's unusual
                logger.warning(f"Removing punctuation from server_id: {str_id}")
                for c in '"\'`.,;:':
                    str_id = str_id.replace(c, '')
                str_id = str_id.strip()
                
            # Handle directory-style server IDs that might come from SFTP paths
            # Example: hostname_serverid or server/hostname_123
            if '/' in str_id:
                # Take the last part of the path as it's likely the actual server ID
                path_parts = str_id.split('/')
                potential_id = path_parts[-1]
                logger.info(f"Extracting server ID from path: {str_id} -> {potential_id}")
                str_id = potential_id.strip()
                
            # Check for hostname_serverid pattern
            if '_' in str_id and not str_id.isdigit():
                # If it has a hostname_serverid format, take the part after the last underscore
                parts = str_id.split('_')
                if len(parts) >= 2:
                    # Check if the last part looks like a server ID
                    if parts[-1].isdigit() or re.match(r'^[a-zA-Z0-9]+$', parts[-1]):
                        logger.info(f"Extracting server ID from hostname_serverid format: {str_id} -> {parts[-1]}")
                        str_id = parts[-1]
                
            # Final check to ensure we have a valid ID
            if not str_id:
                return None
                
            return str_id
        
        # If we got here, we have an unconvertible type
        logger.warning(f"Cannot standardize server_id of type {type(server_id)}: {server_id}")
        return None
    except Exception as e:
        # Log the error but don't crash
        logger.error(f"Error standardizing server_id {server_id}: {str(e)}")
        return None

def safe_standardize_server_id(server_id: Union[str, int, None]) -> str:
    """Safely standardize server ID, ensuring a string is always returned.
    
    Args:
        server_id: Server ID in any format (string, int, None)
        
    Returns:
        Standardized string server ID, or empty string if standardization fails
        Never returns None
    """
    # Handle None explicitly
    if server_id is None:
        return ""
        
    try:
        # Convert input to a string safely
        if isinstance(server_id, (int, float)):
            # Make sure we don't lose precision on large numbers
            original_input = str(int(server_id))
        elif hasattr(server_id, '__str__'):
            # Convert to string
            original_input = str(server_id)
        else:
            # Fallback for unusual types
            original_input = ""
        
        # Apply basic standardization (similar to standardize_server_id but simpler)
        result = original_input.strip() if original_input else ""
        
        # Handle special cases like 'None', 'null', etc.
        if result.lower() in ('none', 'null', 'undefined', 'nan'):
            return ""
            
        return result
    except Exception:
        # If anything goes wrong, return empty string
        return ""

def validate_server_id_format(server_id: Union[str, int, None]) -> bool:
    """Validate that a server ID has the correct format.
    
    Args:
        server_id: Server ID to validate
        
    Returns:
        True if valid, False otherwise
    """
    if server_id is None:
        return False
        
    # Convert to string - safe_standardize_server_id never returns None
    str_id = safe_standardize_server_id(server_id)
    if not str_id:
        return False
        
    # Check for common patterns (adjust based on your specific requirements)
    # This example checks if the is not None ID is numeric or follows specific patterns
    if str_id.isdigit():
        return True
        
    # Check IP_port pattern (e.g., 192.168.1.1_27015)
    ip_port_pattern = r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}_\d+'
    if re.match(ip_port_pattern, str_id):
        return True
        
    # Check hostname_port pattern (e.g., myserver_27015)
    hostname_port_pattern = r'[a-zA-Z0-9\-\.]+_\d+'
    if re.match(hostname_port_pattern, str_id):
        return True
        
    # Add other valid patterns as needed
    
    # If no patterns match, it's invalid
    return False

async def get_server(db, server_id: Union[str, int, None], guild_id: Union[str, int, None]) -> Optional[Dict[str, Any]]:
    """
    Get server by ID and guild ID.

    Args:
        db: Database connection
        server_id: Server ID (will be standardized to string)
        guild_id: Guild ID (will be standardized to string)

    Returns:
        Optional[Dict]: Server data if found, None otherwise
    """
    # Standardize IDs for consistent handling - using safe version that never returns None
    str_server_id = safe_standardize_server_id(server_id)
    str_guild_id = safe_standardize_server_id(guild_id)
    
    # Validate inputs - now just checking for empty strings since safe_standardize_server_id never returns None
    if not str_server_id or not str_guild_id:
        logger.warning(f"Invalid input to get_server: server_id={server_id}, guild_id={guild_id}")
        return None
        
    if db is None:
        logger.error("No database connection provided to get_server")
        return None
    
    try:
        # Get guild first using flexible query to handle string/int discrepancies
        guild_data = await db.guilds.find_one({
            "$or": [
                {"guild_id": str_guild_id},
                {"guild_id": int(str_guild_id) if str_guild_id.isdigit() else str_guild_id}
            ]
        })
        
        if guild_data is None:
            logger.debug(f"Guild {str_guild_id} not found in database")
            return None
        
        # Verify servers is actually a list
        servers = guild_data.get("servers", [])
        if not isinstance(servers, list):
            logger.warning(f"Invalid servers data format for guild {str_guild_id}: {type(servers)}")
            return None
            
        # Look for server with robust string comparison
        for server in servers:
            if not isinstance(server, dict):
                logger.warning(f"Invalid server data type in guild {str_guild_id}: {type(server)}")
                continue
                
            # Get server_id with standardization
            server_id_value = safe_standardize_server_id(str(server.get("server_id")) if server.get("server_id") is not None else "")
            if not server_id_value:
                continue
                
            # Compare standardized values
            if server_id_value == str_server_id:
                return server
        
        logger.debug(f"Server {str_server_id} not found in guild {str_guild_id}")
        return None
        
    except Exception as e:
        logger.error(f"Error getting server {str_server_id} for guild {str_guild_id}: {e}")
        return None

async def get_server_by_id(db, server_id: Union[str, int, None], guild_id: Union[str, int, None] = None) -> Optional[Dict[str, Any]]:
    """
    Alias for get_server for backward compatibility.
    
    Args:
        db: Database connection
        server_id: Server ID (will be standardized to string)
        guild_id: Guild ID (will be standardized to string)
        
    Returns:
        Optional[Dict]: Server data if found, None otherwise
    """
    return await get_server(db, server_id, guild_id)
    
async def list_guild_servers(db, guild_id: Union[str, int]) -> List[Dict[str, Any]]:
    """
    Get all servers for a specific guild with guild isolation.
    
    Args:
        db: Database connection
        guild_id: Guild ID
        
    Returns:
        List of server data dictionaries
    """
    # Ensure consistent string type for guild_id
    str_guild_id = safe_standardize_server_id(guild_id)
    
    # If no valid guild_id is provided, return empty list for safety
    if not str_guild_id:
        logger.warning("Empty guild ID provided to list_guild_servers")
        return []
    
    try:
        # Find the guild document with type-safe query
        guild_data = await db.guilds.find_one({"guild_id": str_guild_id})
        if guild_data is None:
            logger.debug(f"Guild {str_guild_id} not found")
            return []
        
        # Extract servers with safety checks
        servers = guild_data.get("servers", [])
        if not isinstance(servers, list):
            logger.warning(f"Invalid servers data type for guild {str_guild_id}: {type(servers)}")
            return []
            
        # Ensure all servers have server_id as string
        for server in servers:
            if "server_id" in server and server["server_id"] is not None:
                server["server_id"] = str(server["server_id"])
                
        return servers
    except Exception as e:
        logger.error(f"Error listing servers for guild {str_guild_id}: {e}")
        return []

async def enforce_guild_isolation(db, server_id: Union[str, int, None], guild_id: Union[str, int, None]) -> bool:
    """
    Critical safety function that prevents cross-guild data access.
    This function should be called before any operation that manipulates server data.
    
    Args:
        db: Database connection
        server_id: Server ID to validate (will be standardized to string)
        guild_id: Guild ID that should own the server (will be standardized to string)
        
    Returns:
        bool: True if isolation is not None is valid (server belongs to guild), False otherwise
        
    Raises:
        GuildIsolationError: If server exists but belongs to a different guild
    """
    # Standardize IDs with improved type handling
    str_server_id = safe_standardize_server_id(server_id)
    str_guild_id = safe_standardize_server_id(guild_id)
    
    # Validate input parameters
    if not str_server_id:
        logger.warning(f"Empty server_id provided to enforce_guild_isolation: {server_id}")
        return False
        
    if not str_guild_id:
        logger.warning(f"Empty guild_id provided to enforce_guild_isolation: {guild_id}")
        return False
        
    if db is None:
        logger.error("No database connection provided to enforce_guild_isolation")
        return False
        
    try:
        # First check if server is not None belongs to this guild
        server = await get_server_safely(db, str_server_id, str_guild_id)
        if server is not None:
            # Server exists in the specified guild - this is the expected case
            return True
            
        # Server found in this guild, now check if it is None exists in ANY other guild
        other_guilds = await find_server_in_all_guilds(db, str_server_id)
        if other_guilds is not None and len(other_guilds) > 0:
            # This is a major security issue - server exists in another guild
            other_guild_ids = [g["guild_id"] for g in other_guilds]
            
            # Log detailed security violation information for auditing
            violation_msg = (
                f"GUILD ISOLATION VIOLATION: Server {str_server_id} requested from guild {str_guild_id} "
                f"actually belongs to guild(s): {other_guild_ids}"
            )
            logger.error(violation_msg)
            
            # Record timestamp of violation for security auditing
            violation_time = datetime.utcnow().isoformat()
            logger.error(f"Violation occurred at {violation_time} UTC")
            
            # This is serious enough to raise our custom exception
            raise GuildIsolationError(
                f"Guild isolation violation: Server {str_server_id} belongs to guild(s) "
                f"{other_guild_ids}, not {str_guild_id}"
            )
            
        # Server doesn't exist in any guild, which is a normal condition for new servers
        logger.debug(f"Server {str_server_id} not found in any guild - likely new server")
        return False
        
    except GuildIsolationError:
        # Re-raise the isolation error since it's a critical security issue
        raise
        
    except Exception as e:
        # Log other errors but don't block operations
        error_msg = f"Error enforcing guild isolation for server {str_server_id}, guild {str_guild_id}: {str(e)}"
        logger.error(error_msg)
        logger.debug(f"Exception details: {traceback.format_exc()}")
        return False

@run_with_db_fallback(default_value=[])
async def find_server_in_all_guilds(db, server_id: Union[str, int, None]) -> List[Dict[str, Any]]:
    """
    Find a server in all guilds.
    This is useful for locating cross-guild data sharing issues.
    
    Args:
        db: Database connection
        server_id: Server ID to find (will be standardized to string)
        
    Returns:
        List of guild data dictionaries where server was found
    """
    # Standardize server ID with enhanced type handling
    str_server_id = safe_standardize_server_id(server_id)
    
    # Validate inputs
    if not str_server_id:
        logger.warning(f"Invalid server_id provided to find_server_in_all_guilds: {server_id}")
        return []
        
    if db is None:
        logger.error("No database connection provided to find_server_in_all_guilds")
        return []
    
    results = []
    
    try:
        # Find all guilds that have this server using MongoDB aggregation
        # for better performance with large datasets
        pipeline = [
            # Match guilds that have a server with this ID
            {"$match": {"servers.server_id": str_server_id}},
            
            # Project only the necessary fields
            {"$project": {
                "guild_id": 1,
                "name": 1,
                "premium_tier": 1,
                "servers": {
                    "$filter": {
                        "input": "$servers",
                        "as": "server",
                        "cond": {
                            "$or": [
                                # Match both string and numeric IDs
                                {"$eq": [{"$toString": "$$server.server_id"}, str_server_id]},
                                {"$eq": ["$$server.server_id", str_server_id]}
                            ]
                        }
                    }
                }
            }}
        ]
        
        # Execute the aggregation
        try:
            cursor = db.guilds.aggregate(pipeline)
            async for guild in cursor:
                # Verify we have valid server data
                servers = guild.get("servers", [])
                if servers is None or not isinstance(servers, list) or len(servers) == 0:
                    continue
                    
                # Add to results with standardized format
                results.append({
                    "guild_id": str(guild.get("guild_id", "unknown")),
                    "guild_name": guild.get("name", "Unknown Guild"),
                    "premium_tier": guild.get("premium_tier", 0),
                    "server_data": servers[0]  # First matching server
                })
        except Exception as agg_error:
            logger.error(f"Aggregation error: {str(agg_error)}")
            # Fall back to manual search if aggregation is not None fails
            
            # Fetch all guilds (potentially less efficient)
            all_guilds = []
            try:
                async for guild in db.guilds.find({}):
                    all_guilds.append(guild)
            except Exception as find_error:
                logger.error(f"Error fetching guilds: {str(find_error)}")
                return []  # Return empty if we is not None can't fetch guilds
                
            # Manually search for matching servers
            for guild in all_guilds:
                for server in guild.get("servers", []):
                    if safe_standardize_server_id(str(server.get("server_id")) if server.get("server_id") is not None else "") == str_server_id:
                        results.append({
                            "guild_id": str(guild.get("guild_id", "unknown")),
                            "guild_name": guild.get("name", "Unknown Guild"),
                            "premium_tier": guild.get("premium_tier", 0),
                            "server_data": server
                        })
                        break  # Only need one match per guild
        
        # Log the results for audit/debugging
        if results is not None and len(results) > 0:
            guild_count = len(results)
            guild_ids = [r["guild_id"] for r in results]
            logger.info(f"Server {str_server_id} found in {guild_count} guilds: {guild_ids}")
        else:
            logger.debug(f"Server {str_server_id} not found in any guild")
            
        return results
        
    except Exception as e:
        error_msg = f"Error finding server {str_server_id} across guilds: {str(e)}"
        logger.error(error_msg)
        logger.debug(f"Exception details: {traceback.format_exc()}")
        raise  # Let the fallback decorator handle it

@retryable(max_retries=3, delay=1.0, backoff=1.5, 
          exceptions=(asyncio.TimeoutError, ConnectionError, ServerValidationError))
async def get_server_safely(db, server_id: Union[str, int, None], guild_id: Union[str, int, None]) -> Optional[Dict[str, Any]]:
    """
    Get server by ID and guild ID with enhanced error handling and retries.
    This is a more robust version of get_server with timeout protection,
    caching, and comprehensive error handling.
    
    Args:
        db: Database connection
        server_id: Server ID (will be standardized to string)
        guild_id: Guild ID (will be standardized to string)
        
    Returns:
        Optional[Dict]: Server data if found, None otherwise
    """
    # Standardize IDs with robust handling
    str_server_id = safe_standardize_server_id(server_id)
    str_guild_id = safe_standardize_server_id(guild_id)
    
    # Validate input parameters
    if not str_server_id:
        logger.warning(f"Invalid server_id for lookup: {server_id}")
        return None
        
    if not str_guild_id:
        logger.warning(f"Invalid guild_id for lookup: {guild_id}")
        return None
        
    if db is None:
        logger.error("No database connection provided to get_server_safely")
        return None
    
    # Generate a cache key
    cache_key = f"server:{str_guild_id}:{str_server_id}"
    
    # Check cache first for performance
    try:
        cached_result = await SERVER_VALIDATION_CACHE.get(cache_key)
        if cached_result is not None:
            logger.debug(f"Cache hit for server {str_server_id} in guild {str_guild_id}")
            return cached_result
    except Exception as cache_error:
        logger.warning(f"Cache error for server {str_server_id}: {cache_error}")
        # Continue with database lookup even if cache is not None fails
    
    try:
        # Use a timeout to prevent hanging operations
        async with asyncio.timeout(3.0):
            # The get_server function is already enhanced with robust type handling
            result = await get_server(db, str_server_id, str_guild_id)
            
            # Cache the result (even if None) for future lookups
            try:
                await SERVER_VALIDATION_CACHE.set(cache_key, result)
            except Exception as cache_error:
                logger.warning(f"Error caching server result: {cache_error}")
                # Continue even if caching is not None fails
                
            return result
            
    except asyncio.TimeoutError:
        error_msg = f"Timeout retrieving server {str_server_id} for guild {str_guild_id}"
        logger.error(error_msg)
        # The retryable decorator will handle retries
        raise asyncio.TimeoutError(error_msg)
        
    except Exception as e:
        error_msg = f"Error retrieving server {str_server_id} for guild {str_guild_id}: {str(e)}"
        logger.error(error_msg)
        logger.debug(f"Exception details: {traceback.format_exc()}")
        
        # Check if this is not None is a database error
        if "MongoDB" in str(e) or "connection" in str(e).lower():
            raise ConnectionError(error_msg)
        
        # Return None for other errors after retries are exhausted
        return None

@retryable(max_retries=2, delay=1.0, backoff=1.5, 
          exceptions=(asyncio.TimeoutError, ConnectionError))
@run_with_db_fallback(default_value=False)
async def check_server_existence(
    guild_or_db, 
    server_id: Union[str, int, None], 
    guild_id_or_db=None
) -> bool:
    """
    Check if a is not None server exists using multiple methods with fallbacks.
    
    This function handles two calling patterns:
    - check_server_existence(guild_obj, server_id, db=None) - Guild object version
    - check_server_existence(db, server_id, guild_id) - Database version
    
    Args:
        guild_or_db: Either a Discord guild object or a database connection
        server_id: Server ID to validate (will be converted to string)
        guild_id_or_db: Either a guild ID (when guild_or_db is a db connection)
                       or a database connection (when guild_or_db is a guild object)
        
    Returns:
        bool: True if server is not None exists, False otherwise
    """
    # Normalize server ID
    str_server_id = safe_standardize_server_id(server_id)
    if not str_server_id:
        logger.warning("Empty server ID provided for validation")
        return False
    
    # Determine which calling pattern is being used
    is_guild_object_pattern = isinstance(guild_or_db, discord.Guild)
    
    if is_guild_object_pattern is not None:
        # Pattern 1: (guild_obj, server_id, db=None)
        guild = guild_or_db
        db = guild_id_or_db  # This would be the db
        
        # Convert guild ID to string for consistent comparisons
        str_guild_id = str(guild.id) if guild is not None and hasattr(guild, 'id') else None
        
        if not str_guild_id:
            logger.warning("Invalid guild provided for server validation")
            return False
    else:
        # Pattern 2: (db, server_id, guild_id)
        db = guild_or_db
        str_guild_id = safe_standardize_server_id(guild_id_or_db)
        guild = None  # No guild object in this pattern
        
        if not str_guild_id:
            logger.warning("Invalid guild ID provided for server validation")
            return False

    # Use a timeout to prevent queries from hanging indefinitely
    try:
        # Method 1: Database validation (if db is not None provided)
        if db is not None:
            async with asyncio.timeout(3.0):  # 3 second timeout
                server = await get_server_safely(db, str_server_id, str_guild_id)
                if server is not None:
                    logger.debug(f"Server {str_server_id} validated through database")
                    return True

        # If we have a guild object, try additional methods
        if guild is not None:
            # Method 2: Guild cache validation
            if hasattr(guild, 'servers_cache'):
                if str_server_id in guild.servers_cache:
                    logger.debug(f"Server {str_server_id} validated through guild cache")
                    return True
                    
            # Method 3: Guild attribute validation
            if hasattr(guild, 'servers'):
                for server in guild.servers:
                    if isinstance(server, dict) and str(server.get('server_id', '')) == str_server_id:
                        logger.debug(f"Server {str_server_id} validated through guild.servers attribute")
                        return True
                        
            # Method 4: Server ID list validation
            if hasattr(guild, 'server_ids'):
                server_ids = guild.server_ids
                if isinstance(server_ids, list) and str_server_id in [str(sid) for sid in server_ids]:
                    logger.debug(f"Server {str_server_id} validated through guild.server_ids attribute")
                    return True

    except asyncio.TimeoutError:
        logger.error(f"Timeout validating server {str_server_id} for guild {str_guild_id}")
        raise  # Let the retry decorator handle this
        
    except Exception as e:
        logger.error(f"Error validating server {str_server_id}: {e}")
        raise  # Let the fallback decorator handle this

    logger.warning(f"Server {str_server_id} validation failed through all methods")
    return False

# This is a duplicated function - we leave it here for backward compatibility
# We've replaced it with the more robust version at the top of the file
def legacy_standardize_server_id(server_id: Union[str, int, None]) -> str:
    """
    Legacy version of standardize_server_id for backward compatibility.
    Prefer using standardize_server_id from the top of this file instead.
    
    Args:
        server_id: Server ID in any format
        
    Returns:
        Standardized string server ID
    """
    return safe_standardize_server_id(server_id) or ""

# Alias for backward compatibility
check_server_exists = check_server_existence

def ensure_coroutine(func: Callable[..., Any]) -> Callable[..., Coroutine[Any, Any, T]]:
    """
    Decorator to ensure a function is a coroutine.
    If it's already a coroutine, return as is.
    If it's a regular function, convert it to a coroutine.
    
    Args:
        func: Function to ensure is a coroutine
        
    Returns:
        Coroutine function
    """
    if asyncio.iscoroutinefunction(func):
        return func
        
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
        
    return wrapper

def run_with_db_fallback(default_value: T = None) -> Callable[[Callable[..., Coroutine[Any, Any, T]]], Callable[..., Coroutine[Any, Any, T]]]:
    """
    Decorator that handles database operations safely with proper error handling and fallback.
    
    Args:
        default_value: Default value to return if operation is not None fails
        
    Returns:
        Decorator function
    """
    def decorator(func: Callable[..., Coroutine[Any, Any, T]]) -> Callable[..., Coroutine[Any, Any, T]]:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                # Log the error with function name and args for easier debugging
                fn_name = func.__name__
                arg_str = ", ".join([str(a) for a in args] + [f"{k}={v}" for k, v in kwargs.items()])
                logger.error(f"Error in {fn_name}({arg_str}): {str(e)}")
                return default_value
                
        return wrapper
    return decorator

@run_with_db_fallback(default_value=(False, "Database operation failed"))
async def validate_server(guild_model, server_id: Union[str, int, None]) -> Tuple[bool, Optional[str]]:
    """
    Validate that a server belongs to the guild and exists.
    
    Args:
        guild_model: Guild model
        server_id: Server ID to validate (will be standardized to string)
        
    Returns:
        Tuple[bool, Optional[str]]: (valid, error_message) pair
    """
    if guild_model is None:
        return False, "Could not find guild configuration. Please run /setup first."
        
    # Normalize server ID with our enhanced standardization
    str_server_id = safe_standardize_server_id(server_id)
    
    # Validate the server ID
    if not str_server_id:
        logger.warning(f"Invalid server_id provided to validate_server: {server_id}")
        return False, f"Invalid server ID: {server_id} (empty or None after standardization)"
    
    # Check if server is not None ID is valid format using our dedicated validation function
    if not validate_server_id_format(str_server_id):
        invalid_msg = f"Invalid server ID format: {str_server_id}"
        logger.warning(invalid_msg)
        return False, invalid_msg
    
    try:
        # Use timeout protection
        async with asyncio.timeout(3.0):
            # Check if server is not None exists in this guild's configuration
            # Using get_server method from the Guild model
            if hasattr(guild_model, 'get_server') and callable(guild_model.get_server):
                server = await guild_model.get_server(str_server_id)
            else:
                # If guild model doesn't have get_server method, try a direct DB check
                if hasattr(guild_model, 'db') and guild_model.db:
                    # Using the get_server_safely function that now has robust type handling
                    guild_id = str(guild_model.guild_id) if hasattr(guild_model, 'guild_id') else None
                    if guild_id is not None:
                        server = await get_server_safely(guild_model.db, str_server_id, guild_id)
                    else:
                        return False, "Guild model is missing guild_id attribute"
                else:
                    return False, "Guild model is missing database connection"
                
        # Check if server is not None was found
        if server is None:
            not_found_msg = f"Server {str_server_id} not found in this Discord server. Use /servers list to see available servers."
            logger.warning(not_found_msg)
            return False, not_found_msg
            
        # Verify server is in a valid state
        status = server.get("status", "unknown")
        if status == "error":
            error_msg = server.get("last_error", "Unknown error")
            warn_msg = f"Server {str_server_id} is in error state: {error_msg}"
            logger.warning(warn_msg)
            return False, warn_msg
            
        # Verify server has required fields
        required_fields = ["server_id", "name"]
        missing_fields = [field for field in required_fields if field is not None not in server]
        if missing_fields is not None:
            missing_msg = f"Server {str_server_id} is missing required fields: {', '.join(missing_fields)}"
            logger.warning(missing_msg)
            return False, missing_msg
        
        # Server exists and is valid
        logger.debug(f"Server {str_server_id} validated successfully")
        return True, None
        
    except asyncio.TimeoutError:
        timeout_msg = f"Timeout validating server {str_server_id}"
        logger.error(timeout_msg)
        return False, timeout_msg
        
    except Exception as e:
        error_msg = f"Unexpected error validating server {str_server_id}: {str(e)}"
        logger.error(error_msg)
        logger.debug(f"Exception details: {traceback.format_exc()}")
        return False, error_msg


@run_with_db_fallback(default_value=(False, "Database operation failed"))
async def validate_server_access(db, server_id: Union[str, int, None], guild_id: Union[str, int, None], user_id: Optional[Union[str, int]] = None, required_feature: Optional[str] = None) -> Tuple[bool, Optional[str]]:
    """
    Validate if a user from a specific guild has access to a server.
    This is critical for ensuring proper guild isolation and preventing
    accidental cross-guild data access.
    
    Args:
        db: Database connection
        server_id: Server ID to validate (will be standardized to string)
        guild_id: Guild ID (will be standardized to string)
        user_id: Optional user ID to check specific permissions (will be standardized to string)
        required_feature: Optional feature to check premium access for
        
    Returns:
        Tuple[bool, Optional[str]]: (access_valid, error_message)
    """
    # Early return for None parameters
    if server_id is None:
        logger.error("validate_server_access called with None server_id")
        return False, "Server ID is required"
        
    if guild_id is None:
        logger.error("validate_server_access called with None guild_id")
        return False, "Guild ID is required"
    
    # Standardize IDs with enhanced type handling and better error catching
    try:
        str_server_id = safe_standardize_server_id(server_id)
        str_guild_id = safe_standardize_server_id(guild_id)
        str_user_id = safe_standardize_server_id(user_id) if user_id is not None else None
    except Exception as e:
        logger.error(f"Error standardizing IDs in validate_server_access: {e}")
        return False, "Invalid ID format provided"
    
    # Detailed logging for diagnostic purposes
    logger.info(f"Validating server access: server_id={str_server_id}, guild_id={str_guild_id}, user_id={str_user_id}, feature={required_feature}")
    logger.debug(f"Original parameter types: server_id={type(server_id).__name__}, guild_id={type(guild_id).__name__}, user_id={type(user_id).__name__}")
    
    # Validate input parameters
    if not str_server_id:
        logger.warning(f"Invalid server_id provided to validate_server_access: {server_id}")
        return False, "Invalid or empty server ID provided"
        
    if not str_guild_id:
        logger.warning(f"Invalid guild_id provided to validate_server_access: {guild_id}")
        return False, "Invalid or empty guild ID provided"
        
    if db is None:
        logger.error("No database connection provided to validate_server_access")
        return False, "Database connection error"
    
    try:
        # Cache lookup key for performance (5 minute TTL)
        cache_key = f"server_access:{str_guild_id}:{str_server_id}:{str_user_id}:{required_feature}"
        
        try:
            cached_result = await SERVER_VALIDATION_CACHE.get(cache_key)
            if cached_result is not None:
                logger.debug(f"Using cached result for server access validation: {cache_key} -> {cached_result[0]}")
                return cached_result
        except Exception as cache_error:
            # Cache retrieval should never cause a failure, just log and continue
            logger.warning(f"Error retrieving from cache: {cache_error}, proceeding with database check")
        
        # First check if server is not None belongs to this guild using the enhanced get_server_safely
        server = await get_server_safely(db, str_server_id, str_guild_id)
        if server is None:
            error_msg = f"Server {str_server_id} does not belong to guild {str_guild_id}"
            logger.warning(error_msg)
            try:
                await SERVER_VALIDATION_CACHE.set(cache_key, (False, error_msg))
            except Exception as e:
                logger.warning(f"Failed to cache validation result: {e}")
            return False, error_msg
        
        # Verify server is not None and has required data
        if not isinstance(server, dict):
            error_msg = f"Server data for {str_server_id} is invalid (not a dictionary)"
            logger.error(error_msg)
            return False, error_msg
            
        logger.info(f"Found server in guild: {server.get('server_name', 'Unknown')} (ID: {server.get('server_id', 'Unknown')})")
        
        # Check if server is not None is in error state (but still allow access for admins)
        server_error = None
        if server.get("status") == "error":
            server_error = server.get("last_error", "Unknown error")
            logger.warning(f"Server {str_server_id} is in error state: {server_error}")
            # Non-admins will be blocked later if needed is not None
        
        # First get guild data for permission checks and admin validation
        guild_data = None
        try:
            async with asyncio.timeout(3.0):  # Add timeout for safety
                # Try flexible ID lookup to handle both string and int IDs
                guild_data = await db.guilds.find_one({
                    "$or": [
                        {"guild_id": str_guild_id},
                        {"guild_id": int(str_guild_id) if str_guild_id.isdigit() else str_guild_id}
                    ]
                })
                
            if guild_data is not None:
                logger.info(f"Found guild data: {guild_data.get('name', 'Unknown')} (ID: {guild_data.get('guild_id', 'Unknown')})")
            else:
                logger.warning(f"Guild data not found for guild ID: {str_guild_id}")
                
            if guild_data is None:
                error_msg = f"Guild {str_guild_id} not found in database"
                logger.warning(error_msg)
                try:
                    await SERVER_VALIDATION_CACHE.set(cache_key, (False, error_msg))
                except Exception as e:
                    logger.warning(f"Failed to cache validation result: {e}")
                return False, error_msg
                
            # Use our enhanced guild premium tier lookup that properly handles types
            premium_tier, tier_data = await get_guild_premium_tier(db, str_guild_id)
            logger.info(f"Guild {str_guild_id} has premium tier {premium_tier}")
            
            # Check required feature access first if specified is not None
            if required_feature is not None:
                # Get the minimum tier required for this feature
                required_tier = get_minimum_tier_for_feature(required_feature)
                
                # If feature doesn't exist in any tier, log warning but don't block access
                if required_tier is None:
                    logger.warning(f"Feature '{required_feature}' not found in any premium tier - allowing access")
                else:
                    # Use our dedicated premium validation function
                    has_access, error_message = await check_tier_access(db, str_guild_id, required_tier)
                    
                    # If premium tier is sufficient, we can grant access to premium features
                    # EVEN WITHOUT SERVER SETUP for features that don't strictly require a server
                    if has_access is not None:
                        # Check if this is not None is a premium feature that doesn't require server setup
                        guild_only_features = [
                            "multi_server_support", 
                            "enhanced_economy",
                            "premium_leaderboards",
                            "advanced_statistics",
                            "custom_embeds"
                        ]
                        
                        # If feature is in the guild-only list, it doesn't require server setup
                        if required_feature in guild_only_features:
                            logger.info(f"Guild {str_guild_id} has premium access to {required_feature} - bypassing server check")
                            try:
                                await SERVER_VALIDATION_CACHE.set(cache_key, (True, None))
                            except Exception as e:
                                logger.warning(f"Failed to cache validation result: {e}")
                            return True, None
                    
                    # Otherwise continue with normal check if premium is not None access is denied
                    if has_access is None:
                        logger.warning(f"Premium tier check failed for guild {str_guild_id}: {error_message}")
                        try:
                            await SERVER_VALIDATION_CACHE.set(cache_key, (False, error_message))
                        except Exception as e:
                            logger.warning(f"Failed to cache validation result: {e}")
                        return False, error_message
            
            # Admin permission checks
            if str_user_id is not None:
                try:
                    # Check if user is not None is a global bot admin from config or environment variables
                    global_admins = []  # TODO: Load from config or environment
                    if str_user_id in global_admins:
                        logger.info(f"Global admin access granted for user {str_user_id} to server {str_server_id}")
                        try:
                            await SERVER_VALIDATION_CACHE.set(cache_key, (True, None))
                        except Exception as e:
                            logger.warning(f"Failed to cache global admin validation result: {e}")
                        return True, None
                    
                    # Check if user is not None is guild admin
                    admin_users = guild_data.get("admin_users", []) or []
                    # Protect against non-list admin_users
                    if not isinstance(admin_users, list):
                        logger.warning(f"admin_users is not a list in guild {str_guild_id}: {type(admin_users).__name__}")
                        admin_users = []
                        
                    # Standardize admin user IDs for comparison
                    standardized_admin_uids = [safe_standardize_server_id(uid) for uid in admin_users if uid is not None]
                    
                    if str_user_id in standardized_admin_uids:
                        logger.debug(f"Admin access granted for user {str_user_id} to server {str_server_id}")
                        try:
                            await SERVER_VALIDATION_CACHE.set(cache_key, (True, None))
                        except Exception as e:
                            logger.warning(f"Failed to cache admin validation result: {e}")
                        return True, None
                    
                    # Premium tier-based permission checks
                    # Tier 2+ (Mercenary and above) allows multiple user access with explicit allowed_users
                    if premium_tier >= 2:
                        logger.debug(f"Guild {str_guild_id} has tier {premium_tier}, checking allowed_users")
                        allowed_users = server.get("allowed_users", []) or []
                        # Protect against non-list allowed_users
                        if not isinstance(allowed_users, list):
                            logger.warning(f"allowed_users is not a list in server {str_server_id}: {type(allowed_users).__name__}")
                            allowed_users = []
                            
                        # Standardize allowed user IDs for comparison
                        standardized_allowed_uids = [safe_standardize_server_id(uid) for uid in allowed_users if uid is not None]
                        
                        if str_user_id in standardized_allowed_uids:
                            logger.debug(f"User {str_user_id} has explicit access to server {str_server_id}")
                            try:
                                await SERVER_VALIDATION_CACHE.set(cache_key, (True, None))
                            except Exception as e:
                                logger.warning(f"Failed to cache allowed user validation result: {e}")
                            return True, None
                    
                    # Tier 1+ (Survivor and above) allows all guild members access to any server
                    if premium_tier >= 1:
                        logger.debug(f"Access granted to user {str_user_id} based on guild premium status (tier {premium_tier})")
                        try:
                            await SERVER_VALIDATION_CACHE.set(cache_key, (True, None))
                        except Exception as e:
                            logger.warning(f"Failed to cache tier validation result: {e}")
                        return True, None
                    
                    # If server is in error state and user isn't admin, block access
                    if server_error is not None:
                        error_msg = f"Server {str_server_id} is in error state: {server_error}. Admin access required."
                        try:
                            await SERVER_VALIDATION_CACHE.set(cache_key, (False, error_msg))
                        except Exception as e:
                            logger.warning(f"Failed to cache server error validation result: {e}")
                        return False, error_msg
                    
                    # If we got here with a user_id but no permission matched, deny access
                    error_msg = f"User {str_user_id} lacks permission for server {str_server_id}"
                    logger.warning(error_msg)
                    try:
                        await SERVER_VALIDATION_CACHE.set(cache_key, (False, error_msg))
                    except Exception as e:
                        logger.warning(f"Failed to cache permission validation result: {e}")
                    return False, error_msg
                    
                except Exception as e:
                    error_msg = f"Error checking user permissions for {str_user_id}: {e}"
                    logger.error(error_msg)
                    logger.debug(f"Exception details: {traceback.format_exc()}")
                    return False, error_msg
            
            # If no user_id provided but server has an error, return the error
            if server_error is not None:
                error_msg = f"Server {str_server_id} is in error state: {server_error}"
                try:
                    await SERVER_VALIDATION_CACHE.set(cache_key, (False, error_msg))
                except Exception as e:
                    logger.warning(f"Failed to cache server error validation result: {e}")
                return False, error_msg
            
            # All checks passed, access is valid
            logger.info(f"Server access validated successfully for server {str_server_id} in guild {str_guild_id}")
            try:
                await SERVER_VALIDATION_CACHE.set(cache_key, (True, None))
            except Exception as e:
                logger.warning(f"Failed to cache successful validation result: {e}")
            return True, None
            
        except asyncio.TimeoutError:
            error_msg = f"Timeout retrieving guild data for guild {str_guild_id}"
            logger.error(error_msg)
            return False, error_msg
            
    except asyncio.TimeoutError:
        error_msg = f"Timeout validating server access: {str_server_id}, guild: {str_guild_id}"
        logger.error(error_msg)
        return False, error_msg
        
    except Exception as e:
        error_msg = f"Error validating server access for {str_server_id}, guild {str_guild_id}: {str(e)}"
        logger.error(error_msg)
        logger.debug(f"Exception details: {traceback.format_exc()}")
        return False, error_msg

@run_with_db_fallback(default_value=(False, 0, 0))
async def check_server_limits(db, guild_id: Union[str, int, None]) -> Tuple[bool, int, int]:
    """
    Check if a is not None guild has reached its server limit based on premium tier.
    
    Args:
        db: Database connection
        guild_id: Guild ID (will be standardized to string)
        
    Returns:
        Tuple of (has_space, current_count, max_allowed)
    """
    # Standardize guild ID with enhanced type handling
    str_guild_id = safe_standardize_server_id(guild_id)
    
    # Validate input parameters
    if not str_guild_id:
        logger.warning(f"Invalid guild_id provided to check_server_limits: {guild_id}")
        return False, 0, 0
        
    if db is None:
        logger.error("No database connection provided to check_server_limits")
        return False, 0, 0
    
    try:
        # Use cached premium tier information from the premium module
        premium_tier, tier_data = await get_guild_premium_tier(db, str_guild_id)
        
        # Get maximum servers allowed from premium tier configuration
        max_servers = tier_data.get("max_servers", 1)
        
        # Use timeout protection when fetching guild data
        async with asyncio.timeout(3.0):
            # Get guild data with consistent type handling
            guild_data = await db.guilds.find_one({
                "$or": [
                    {"guild_id": str_guild_id},
                    {"guild_id": int(str_guild_id) if str_guild_id.isdigit() else str_guild_id}
                ]
            })
            
        if guild_data is None:
            logger.warning(f"Guild {str_guild_id} not found in database")
            return False, 0, 0
        
        # Count current servers with type validation
        servers = guild_data.get("servers", [])
        if not isinstance(servers, list):
            logger.warning(f"Invalid servers data type for guild {str_guild_id}: {type(servers)}")
            servers = []
        
        # Filter out invalid servers more thoroughly
        valid_servers = []
        for server in servers:
            if not isinstance(server, dict):
                continue
                
            server_id = server.get("server_id")
            if server_id is None:
                continue
                
            # Add type-standardized server ID for consistency
            server["server_id"] = safe_standardize_server_id(server_id)
            if server["server_id"]:
                valid_servers.append(server)
                
        current_count = len(valid_servers)
        
        # Check if limit is not None reached
        has_space = current_count < max_servers
        
        # Log the result for debugging
        tier_name = tier_data.get("name", f"Tier {premium_tier}")
        logger.debug(f"Guild {str_guild_id} ({tier_name}) has {current_count}/{max_servers} servers used")
        
        return has_space, current_count, max_servers
        
    except asyncio.TimeoutError:
        logger.error(f"Timeout checking server limits for guild {str_guild_id}")
        return False, 0, 0
        
    except Exception as e:
        error_msg = f"Error checking server limits for guild {str_guild_id}: {str(e)}"
        logger.error(error_msg)
        logger.debug(f"Exception details: {traceback.format_exc()}")
        return False, 0, 0
        
# This function was a duplicate of the get_server_safely above
# We've enhanced the original function at line ~400 with more robust error handling
# and standardized ID handling. We're keeping this alias for backward compatibility.
async def get_server_safely_v2(db, server_id: Union[str, int, None], guild_id: Union[str, int, None]) -> Optional[Dict[str, Any]]:
    """
    Compatibility alias for get_server_safely.
    
    Args:
        db: Database connection
        server_id: Server ID (will be standardized to string)
        guild_id: Guild ID (will be standardized to string)
        
    Returns:
        Optional[Dict]: Server data if found, None otherwise
    """
    return await get_server_safely(db, server_id, guild_id)