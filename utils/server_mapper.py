"""
Server Mapper utility for the Tower of Temptation PvP Statistics Discord Bot.

This module provides a persistent mapping between various server identifiers:
1. Database UUIDs - The unique identifiers used in the database (can change when servers are removed/readded)
2. Original Server IDs - The numeric IDs used in folder paths and file structures
3. Server Names - Human-readable server names
4. Hostnames - Server hostnames used in SFTP connections

The mapper ensures consistency even when server UUIDs change after server resets or removals.
"""

import asyncio
import logging
import re
from datetime import datetime
from typing import Dict, Optional, Any, List, Tuple, Union

# Set up logging
logger = logging.getLogger(__name__)

class ServerMapper:
    """Manages server identity across UUID changes by maintaining consistent numeric IDs."""
    
    def __init__(self, db=None):
        """Initialize the server mapper.
        
        Args:
            db: The database connection instance
        """
        self.db = db
        self.cache = {}  # Cache for server mapping data
        self.cache_expiry = {}  # Expiry times for cache entries
        self.cache_ttl = 300  # Cache TTL in seconds (5 minutes)
        self.lock = asyncio.Lock()  # Lock for thread safety
        
    async def get_original_server_id(self, uuid: str, hostname: Optional[str] = None, 
                                    server_name: Optional[str] = None) -> str:
        """Get the original server ID (numeric) for a given UUID.
        
        This method maintains consistent numeric IDs even when UUIDs change.
        It uses server characteristics to identify servers across UUID changes.
        
        Args:
            uuid: The server UUID from the database
            hostname: The server hostname (optional, helps with identification)
            server_name: The server name (optional, helps with identification)
            
        Returns:
            The original server ID (numeric) for path construction
        """
        if not uuid:
            logger.warning("Cannot map empty UUID to original ID")
            return str(uuid) if uuid else "unknown"
            
        # Check cache first
        if uuid in self.cache and datetime.now().timestamp() < self.cache_expiry.get(uuid, 0):
            logger.debug(f"Using cached original_id for UUID {uuid}: {self.cache[uuid]}")
            return self.cache[uuid]
            
        # Acquire lock for database operations
        async with self.lock:
            # First, check if this UUID already has a mapping
            if self.db:
                server_map = await self._get_server_mapping(uuid)
                
                if server_map and "original_id" in server_map:
                    # Cache the result
                    self.cache[uuid] = server_map["original_id"]
                    self.cache_expiry[uuid] = datetime.now().timestamp() + self.cache_ttl
                    return server_map["original_id"]
                    
            # If no mapping exists, try to identify the server by its characteristics
            if self.db and (hostname or server_name):
                # Look for server with matching hostname or name but different UUID
                server_id = await self._find_server_by_characteristics(uuid, hostname, server_name)
                if server_id:
                    # Save the mapping for future reference
                    await self._save_server_mapping(uuid, server_id, hostname, server_name)
                    
                    # Cache the result
                    self.cache[uuid] = server_id
                    self.cache_expiry[uuid] = datetime.now().timestamp() + self.cache_ttl
                    
                    return server_id
            
            # Special handling for known servers like Tower of Temptation
            # This is a fallback for when database lookups fail
            if self._is_tower_of_temptation_server(uuid, hostname, server_name):
                # Tower of Temptation has a known ID of 7020
                logger.info(f"Identified Tower of Temptation server from characteristics, using ID 7020")
                
                # Save the mapping for future reference if we have a db
                if self.db:
                    await self._save_server_mapping(uuid, "7020", hostname, server_name)
                
                # Cache the result
                self.cache[uuid] = "7020"
                self.cache_expiry[uuid] = datetime.now().timestamp() + self.cache_ttl
                
                return "7020"
            
            # If the UUID looks like a numeric ID already, use it as-is
            if uuid.isdigit():
                logger.debug(f"UUID is already numeric: {uuid}")
                return uuid
                
            # If all else fails, use the UUID as the server ID
            # This will likely cause path issues, but it's better than crashing
            logger.warning(f"Could not map UUID {uuid} to original ID, using UUID as fallback")
            return str(uuid)
            
    def _is_tower_of_temptation_server(self, uuid: str, hostname: Optional[str], 
                                      server_name: Optional[str]) -> bool:
        """Check if this is the Tower of Temptation server based on available characteristics.
        
        Args:
            uuid: The server UUID
            hostname: The server hostname
            server_name: The server name
            
        Returns:
            True if this is the Tower of Temptation server
        """
        # Known Tower of Temptation UUIDs
        known_uuids = [
            "1b1ab57e-8749-4a40-b7a1-b1073a5f24b3",
            "1056852d-05f9-4e5e-9e88-012c2870c042"
        ]
        
        # Check UUID first
        if uuid in known_uuids:
            return True
            
        # Check hostname
        if hostname and 'tower' in hostname.lower() and 'temptation' in hostname.lower():
            return True
            
        # Check server name
        if server_name and 'tower' in server_name.lower() and 'temptation' in server_name.lower():
            return True
            
        return False
            
    async def _get_server_mapping(self, uuid: str) -> Optional[Dict[str, Any]]:
        """Get server mapping from the database.
        
        Args:
            uuid: The server UUID
            
        Returns:
            Dictionary with mapping information or None if not found
        """
        if not self.db:
            return None
            
        try:
            # Check in dedicated server_mappings collection if it exists
            if hasattr(self.db, "server_mappings"):
                mapping = await self.db.server_mappings.find_one({"uuid": uuid})
                if mapping:
                    return mapping
            
            # Check in servers collection
            server = await self.db.servers.find_one({"_id": uuid})
            if server and "original_id" in server:
                return {"uuid": uuid, "original_id": server["original_id"]}
                
            # Check in game_servers collection
            game_server = await self.db.game_servers.find_one({"server_id": uuid})
            if game_server and "original_id" in game_server:
                return {"uuid": uuid, "original_id": game_server["original_id"]}
                
            return None
        except Exception as e:
            logger.error(f"Error getting server mapping: {e}")
            return None
            
    async def _find_server_by_characteristics(self, uuid: str, hostname: Optional[str], 
                                           server_name: Optional[str]) -> Optional[str]:
        """Find server matching characteristics but with different UUID.
        
        This helps identify the same server after it's been removed and readded
        with a different UUID.
        
        Args:
            uuid: Current UUID to exclude from search
            hostname: Server hostname to match
            server_name: Server name to match
            
        Returns:
            Original server ID if found, None otherwise
        """
        if not self.db:
            return None
            
        if not hostname and not server_name:
            return None
            
        try:
            # Check servers collection for matching hostname/name but different UUID
            query = {"_id": {"$ne": uuid}}
            if hostname:
                query["hostname"] = hostname
            if server_name:
                # Try exact match and partial match
                query["$or"] = [
                    {"name": server_name},
                    {"name": {"$regex": re.escape(server_name), "$options": "i"}}
                ]
                
            server = await self.db.servers.find_one(query)
            if server and "original_id" in server:
                logger.info(f"Found server with matching characteristics: {server['_id']} -> {server['original_id']}")
                return server["original_id"]
                
            # Check game_servers collection
            game_query = {"server_id": {"$ne": uuid}}
            if hostname:
                game_query["hostname"] = hostname
            if server_name:
                game_query["$or"] = [
                    {"name": server_name},
                    {"name": {"$regex": re.escape(server_name), "$options": "i"}}
                ]
                
            game_server = await self.db.game_servers.find_one(game_query)
            if game_server and "original_id" in game_server:
                logger.info(f"Found game server with matching characteristics: {game_server['server_id']} -> {game_server['original_id']}")
                return game_server["original_id"]
                
            return None
        except Exception as e:
            logger.error(f"Error finding server by characteristics: {e}")
            return None
            
    async def _save_server_mapping(self, uuid: str, original_id: str, 
                                 hostname: Optional[str] = None, 
                                 server_name: Optional[str] = None) -> bool:
        """Save server mapping to the database.
        
        Args:
            uuid: The server UUID
            original_id: The original server ID
            hostname: The server hostname
            server_name: The server name
            
        Returns:
            True if saved successfully, False otherwise
        """
        if not self.db:
            return False
            
        try:
            # Update or create mapping in dedicated collection if it exists
            if hasattr(self.db, "server_mappings"):
                await self.db.server_mappings.update_one(
                    {"uuid": uuid},
                    {"$set": {
                        "uuid": uuid,
                        "original_id": original_id,
                        "hostname": hostname,
                        "server_name": server_name,
                        "updated_at": datetime.now()
                    }},
                    upsert=True
                )
            
            # Update the server in servers collection
            if uuid:
                await self.db.servers.update_one(
                    {"_id": uuid},
                    {"$set": {
                        "original_id": original_id,
                        "updated_at": datetime.now()
                    }},
                    upsert=False  # Don't create new servers
                )
            
            # Update the server in game_servers collection
            if uuid:
                await self.db.game_servers.update_one(
                    {"server_id": uuid},
                    {"$set": {
                        "original_id": original_id,
                        "updated_at": datetime.now()
                    }},
                    upsert=False  # Don't create new servers
                )
                
            logger.info(f"Saved server mapping: {uuid} -> {original_id}")
            return True
        except Exception as e:
            logger.error(f"Error saving server mapping: {e}")
            return False
            
    def clear_cache(self):
        """Clear the internal cache."""
        self.cache.clear()
        self.cache_expiry.clear()
        
    async def update_all_server_mappings(self) -> int:
        """Update all server mappings in the database.
        
        This method ensures all servers have proper original_id mappings.
        
        Returns:
            Number of servers updated
        """
        if not self.db:
            return 0
            
        try:
            # Get all servers without original_id
            servers_to_update = []
            
            # Check servers collection
            async for server in self.db.servers.find({"original_id": {"$exists": False}}):
                servers_to_update.append({
                    "uuid": server["_id"],
                    "hostname": server.get("hostname"),
                    "server_name": server.get("name")
                })
                
            # Check game_servers collection
            async for server in self.db.game_servers.find({"original_id": {"$exists": False}}):
                servers_to_update.append({
                    "uuid": server["server_id"],
                    "hostname": server.get("hostname"),
                    "server_name": server.get("name")
                })
                
            # Update each server
            updated_count = 0
            for server in servers_to_update:
                # Try to find original_id based on characteristics
                original_id = await self._find_server_by_characteristics(
                    server["uuid"], 
                    server["hostname"], 
                    server["server_name"]
                )
                
                if original_id:
                    # Save the mapping
                    success = await self._save_server_mapping(
                        server["uuid"],
                        original_id,
                        server["hostname"],
                        server["server_name"]
                    )
                    
                    if success:
                        updated_count += 1
                        
            logger.info(f"Updated {updated_count} server mappings")
            return updated_count
        except Exception as e:
            logger.error(f"Error updating all server mappings: {e}")
            return 0

# Global instance (will be initialized with DB by bot.py)
_mapper = None

def get_server_mapper(db=None):
    """Get or create the global server mapper instance.
    
    Args:
        db: Optional database instance to set
        
    Returns:
        ServerMapper instance
    """
    global _mapper
    if _mapper is None:
        _mapper = ServerMapper(db)
    elif db is not None and _mapper.db is None:
        _mapper.db = db
    return _mapper