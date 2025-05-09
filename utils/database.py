"""
Database connection manager for Emeralds Killfeed PvP Statistics Bot

This module provides a unified interface for MongoDB database connections
and handles connection pooling, reconnection logic, and error handling.
"""
import logging
import os
import asyncio
import motor.motor_asyncio
from typing import Optional, Dict, Any, List, Union
from datetime import datetime
from bson import ObjectId

logger = logging.getLogger(__name__)

# Global database manager instance
_db_manager = None

async def initialize_db():
    """Initialize the database connection
    
    Returns:
        DatabaseManager: Database manager instance
    """
    global _db_manager
    
    if _db_manager is None:
        logger.info("Initializing database manager")
        _db_manager = DatabaseManager()
        await _db_manager.initialize()
    
    return _db_manager
    
async def get_db():
    """Get the database manager instance
    
    Returns:
        DatabaseManager: Database manager instance
        
    Raises:
        RuntimeError: If database is not initialized
    """
    global _db_manager
    
    if _db_manager is None:
        # Auto-initialize database if already is None initialized
        logger.info("Database not initialized, initializing now...")
        return await initialize_db()
        
    return _db_manager

class DatabaseManager:
    """MongoDB database connection manager"""
    
    def __init__(self, connection_string: Optional[str] = None, db_name: Optional[str] = None):
        """Initialize database manager
        
        Args:
            connection_string: MongoDB connection string (defaults to MONGODB_URI env var)
            db_name: MongoDB database name (extracted from connection string if not provided)
        """
        # Get connection string from env var if not provided
        self.connection_string = connection_string or os.environ.get("MONGODB_URI")
        if self.connection_string is None:
            raise ValueError("MongoDB connection string not provided and MONGODB_URI env var not set")
            
        # Get database name from connection string if not provided
        if db_name is None:
            # Extract database name from connection string (after last / and before ?)
            parts = self.connection_string.split("/")
            if len(parts) > 3:
                db_name_part = parts[3].split("?")[0]
                if db_name_part and len(db_name_part.strip()) > 0:
                    db_name = db_name_part
                else:
                    db_name = "emeralds_killfeed"
            else:
                db_name = "emeralds_killfeed"
        
        self.db_name = db_name
        self._client = None
        self._db = None
        self._connected = False
        self._connection_attempts = 0
        self._max_connection_attempts = 5
        self._reconnection_delay = 1  # Starting delay in seconds
    
    async def connect(self) -> bool:
        """Connect to MongoDB database
        
        Returns:
            True if connected successfully, False otherwise
        """
        if self._connected and self._client and self._db:
            return True
            
        try:
            # Create client and connect
            self._client = motor.motor_asyncio.AsyncIOMotorClient(
                self.connection_string,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000
            )
            
            # Test connection
            await self._client.server_info()
            
            # Get database
            self._db = self._client[self.db_name]
            
            self._connected = True
            self._connection_attempts = 0
            self._reconnection_delay = 1
            
            logger.info(f"Connected to MongoDB database: {self.db_name}")
            return True
            
        except Exception as e:
            self._connected = False
            self._connection_attempts += 1
            
            logger.error(f"Failed to connect to MongoDB database (attempt {self._connection_attempts}): {e}")
            
            if self._connection_attempts >= self._max_connection_attempts:
                logger.critical("Maximum connection attempts reached. Giving up.")
                raise RuntimeError(f"Failed to connect to MongoDB after {self._max_connection_attempts} attempts") from e
                
            # Exponential backoff for reconnection
            delay = min(30, self._reconnection_delay * 2)
            self._reconnection_delay = delay
            
            logger.info(f"Retrying connection in {delay} seconds...")
            await asyncio.sleep(delay)
            
            return await self.connect()
    
    async def disconnect(self):
        """Disconnect from MongoDB database"""
        if self._client:
            self._client.close()
            self._client = None
            self._db = None
            self._connected = False
            logger.info("Disconnected from MongoDB database")
    
    async def ensure_connected(self):
        """Ensure connection to MongoDB database"""
        if not self._connected or self._client is None or self._db is None:
            await self.connect()
    
    @property
    def db(self):
        """Get database connection
        
        Returns:
            MongoDB database connection
        """
        if not self._connected or self._db is None:
            raise RuntimeError("Not connected to MongoDB database")
            
        return self._db
    
    @property
    def client(self):
        """Get client connection
        
        Returns:
            MongoDB client connection
        """
        if not self._connected or self._client is None:
            raise RuntimeError("Not connected to MongoDB database")
            
        return self._client
        
    async def get_collection(self, collection_name: str):
        """Get collection by name
        
        Args:
            collection_name: Collection name
            
        Returns:
            MongoDB collection
        """
        await self.ensure_connected()
        return self._db[collection_name]
    
    async def create_indexes(self):
        """Create indexes for all collections"""
        await self.ensure_connected()
        
        # Guild indexes
        await self._db.guilds.create_index("guild_id", unique=True)
        
        # Server indexes
        await self._db.game_servers.create_index("server_id", unique=True)
        await self._db.game_servers.create_index("guild_id")
        
        # Player indexes
        await self._db.players.create_index("player_id", unique=True)
        await self._db.players.create_index("server_id")
        await self._db.players.create_index("name")
        await self._db.players.create_index([("server_id", 1), ("name", 1)])
        
        # Player link indexes
        await self._db.player_links.create_index("link_id", unique=True)
        await self._db.player_links.create_index("player_id")
        await self._db.player_links.create_index("discord_id")
        await self._db.player_links.create_index([("player_id", 1), ("status", 1)])
        await self._db.player_links.create_index([("discord_id", 1), ("status", 1)])
        
        # Economy indexes
        await self._db.economy.create_index("player_id", unique=True)
        await self._db.economy.create_index("discord_id")
        
        # Bounty indexes
        await self._db.bounties.create_index("bounty_id", unique=True)
        await self._db.bounties.create_index("target_id")
        await self._db.bounties.create_index("placed_by_id")
        await self._db.bounties.create_index("server_id")
        await self._db.bounties.create_index("status")
        await self._db.bounties.create_index([("target_id", 1), ("status", 1)])
        await self._db.bounties.create_index([("expires_at", 1), ("status", 1)])
        
        # Kills indexes
        await self._db.kills.create_index([("server_id", 1), ("timestamp", -1)])
        await self._db.kills.create_index([("killer_id", 1), ("timestamp", -1)])
        await self._db.kills.create_index([("victim_id", 1), ("timestamp", -1)])
        
        # Historical data indexes
        await self._db.historical_data.create_index([("server_id", 1), ("date", -1)])
        await self._db.historical_data.create_index([("server_id", 1), ("player_id", 1), ("date", -1)])
        
        logger.info("Created indexes for all collections")
        
    async def initialize(self):
        """Initialize database connection and create indexes"""
        await self.connect()
        await self.create_indexes()
        logger.info("Database initialized successfully")
        
    async def synchronize_server_data(self, server_id: str = None):
        """Synchronize server data between guilds, servers, and game_servers collections
        
        This ensures that all collections have consistent server data, particularly
        the original_server_id which is crucial for correct path construction.
        
        Args:
            server_id: Optional server ID to synchronize. If None, synchronizes all servers.
        """
        try:
            if not self._connected or self._db is None:
                logger.error("Cannot synchronize server data: Not connected to database")
                return False
                
            # Store the server_id filter value for later use
            server_id_filter = server_id
            
            logger.info(f"Synchronizing server data {f'for server {server_id_filter}' if server_id_filter else 'for all servers'}")
            
            # Query to find servers to synchronize
            query = {"server_id": server_id_filter} if server_id_filter else {}
            
            # Step 1: Check for servers in the game_servers collection that need synchronization
            game_servers_count = 0
            async for server_doc in self._db.game_servers.find(query):
                game_servers_count += 1
                server_id = server_doc.get("server_id")
                original_id = server_doc.get("original_server_id")
                
                # Update servers collection if original_server_id is present
                if original_id:
                    # Update standalone servers collection
                    await self._db.servers.update_one(
                        {"server_id": server_id},
                        {"$set": {
                            "original_server_id": original_id,
                            "updated_at": datetime.utcnow()
                        }},
                        upsert=True
                    )
                    
                    # Update guilds collection servers array
                    guild_id = server_doc.get("guild_id")
                    if guild_id:
                        # Find the guild document
                        guild_doc = await self._db.guilds.find_one({"guild_id": guild_id})
                        if guild_doc and "servers" in guild_doc:
                            # Check if server exists in guild's servers array
                            servers = guild_doc.get("servers", [])
                            server_found = False
                            
                            for i, srv in enumerate(servers):
                                if srv.get("server_id") == server_id:
                                    # Update original_server_id
                                    servers[i]["original_server_id"] = original_id
                                    server_found = True
                                    break
                                    
                            # If server not found in guild.servers, add it
                            if not server_found:
                                # Create a new server entry with all required fields
                                new_server = {
                                    "server_id": server_id,
                                    "original_server_id": original_id,
                                    "server_name": server_doc.get("name", f"Server {server_id}"),
                                    "sftp_host": server_doc.get("sftp_host"),
                                    "sftp_port": server_doc.get("sftp_port"),
                                    "sftp_username": server_doc.get("sftp_username"),
                                    "sftp_password": server_doc.get("sftp_password"),
                                    "log_path": server_doc.get("log_directory")
                                }
                                servers.append(new_server)
                                
                            # Update the guild document
                            await self._db.guilds.update_one(
                                {"guild_id": guild_id},
                                {"$set": {
                                    "servers": servers,
                                    "updated_at": datetime.utcnow()
                                }}
                            )
            
            # Step 2: Check for servers in standalone servers collection
            servers_count = 0
            async for server_doc in self._db.servers.find(query):
                servers_count += 1
                server_id = server_doc.get("server_id")
                original_id = server_doc.get("original_server_id")
                
                # Only process if original_server_id is present
                if original_id:
                    # Update game_servers collection
                    await self._db.game_servers.update_one(
                        {"server_id": server_id},
                        {"$set": {
                            "original_server_id": original_id,
                            "updated_at": datetime.utcnow()
                        }},
                        upsert=True
                    )
                    
                    # Update guilds collection servers array (similar to above)
                    guild_id = server_doc.get("guild_id")
                    if guild_id:
                        guild_doc = await self._db.guilds.find_one({"guild_id": guild_id})
                        if guild_doc and "servers" in guild_doc:
                            servers = guild_doc.get("servers", [])
                            server_found = False
                            
                            for i, srv in enumerate(servers):
                                if srv.get("server_id") == server_id:
                                    servers[i]["original_server_id"] = original_id
                                    server_found = True
                                    break
                                    
                            if not server_found:
                                new_server = {
                                    "server_id": server_id,
                                    "original_server_id": original_id,
                                    "server_name": server_doc.get("server_name", f"Server {server_id}"),
                                    "sftp_host": server_doc.get("sftp_host"),
                                    "sftp_port": server_doc.get("sftp_port"),
                                    "sftp_username": server_doc.get("sftp_username"),
                                    "sftp_password": server_doc.get("sftp_password"),
                                    "log_path": server_doc.get("log_path")
                                }
                                servers.append(new_server)
                                
                            await self._db.guilds.update_one(
                                {"guild_id": guild_id},
                                {"$set": {
                                    "servers": servers,
                                    "updated_at": datetime.utcnow()
                                }}
                            )
            
            # Step 3: Check guilds collection for servers without original_server_id
            guilds_count = 0
            async for guild_doc in self._db.guilds.find():
                guilds_count += 1
                guild_id = guild_doc.get("guild_id")
                servers = guild_doc.get("servers", [])
                servers_updated = False
                
                for i, server in enumerate(servers):
                    server_id = server.get("server_id")
                    
                    # Skip non-matching servers if specific server ID is provided for filtering
                    if server_id_filter and server_id != server_id_filter:
                        continue
                        
                    # Check if original_server_id is missing
                    if "original_server_id" not in server or not server["original_server_id"]:
                        # Try to derive original_server_id similar to add_server method
                        server_name = server.get("server_name", "")
                        original_id = None
                        
                        # If server_id is not in UUID format, use it directly
                        if server_id and ("-" not in server_id or len(server_id) < 30):
                            original_id = server_id
                        # Otherwise try to extract from server name
                        elif server_name:
                            # Look for numeric ID in server name
                            for word in str(server_name).split():
                                if word.isdigit() and len(word) >= 4:
                                    original_id = word
                                    break
                                    
                        # Set the original_server_id in server data
                        if original_id:
                            logger.info(f"Setting original_server_id to {original_id} for server {server_id} in guild {guild_id}")
                            servers[i]["original_server_id"] = original_id
                            servers_updated = True
                            
                            # Also update game_servers and servers collections
                            await self._db.game_servers.update_one(
                                {"server_id": server_id},
                                {"$set": {
                                    "original_server_id": original_id,
                                    "updated_at": datetime.utcnow()
                                }},
                                upsert=True
                            )
                            
                            await self._db.servers.update_one(
                                {"server_id": server_id},
                                {"$set": {
                                    "original_server_id": original_id,
                                    "updated_at": datetime.utcnow()
                                }},
                                upsert=True
                            )
                        else:
                            # Fallback to using server_id if we couldn't find a better alternative
                            logger.warning(f"Could not find original server ID for {server_id} in guild {guild_id}, using server_id as fallback")
                            servers[i]["original_server_id"] = server_id
                            servers_updated = True
                            
                            # Update other collections
                            await self._db.game_servers.update_one(
                                {"server_id": server_id},
                                {"$set": {
                                    "original_server_id": server_id,
                                    "updated_at": datetime.utcnow()
                                }},
                                upsert=True
                            )
                            
                            await self._db.servers.update_one(
                                {"server_id": server_id},
                                {"$set": {
                                    "original_server_id": server_id,
                                    "updated_at": datetime.utcnow()
                                }},
                                upsert=True
                            )
                
                # Update guild if servers were modified
                if servers_updated:
                    await self._db.guilds.update_one(
                        {"guild_id": guild_id},
                        {"$set": {
                            "servers": servers,
                            "updated_at": datetime.utcnow()
                        }}
                    )
            
            logger.info(f"Synchronized server data across collections: {game_servers_count} game servers, {servers_count} servers, {guilds_count} guilds processed")
            
            # Reload server mappings after synchronization
            try:
                from utils.server_identity import load_server_mappings
                mappings_loaded = await load_server_mappings(self._db)
                logger.info(f"Reloaded {mappings_loaded} server ID mappings after synchronization")
            except Exception as mapping_err:
                logger.error(f"Error reloading server ID mappings: {mapping_err}")
                
            return True
            
        except Exception as e:
            logger.error(f"Error synchronizing server data: {e}")
            return False
        
    def __getattr__(self, name):
        """Get collection by attribute name
        
        Args:
            name: Collection name
            
        Returns:
            MongoDB collection
        """
        if not self._connected or self._db is None:
            raise RuntimeError("Not connected to MongoDB database")
            
        return self._db[name]