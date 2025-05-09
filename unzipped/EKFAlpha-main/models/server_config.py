"""
Server Configuration model for the Tower of Temptation PvP Statistics Discord Bot.

This module provides:
1. Server configuration storage
2. Default settings
3. Configuration validation
4. Discord channel/role management
"""
import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Set, Tuple

from utils.database import get_db
from utils.async_utils import AsyncCache

logger = logging.getLogger(__name__)

class ServerConfig:
    """Server configuration for a Discord guild"""
    
    # Collection name in database
    COLLECTION_NAME = "server_configs"
    
    # Default values
    DEFAULT_VALUES = {
        "prefix": "!",
        "enabled": True,
        "admin_role_id": None,
        "moderator_role_id": None,
        "stats_channel_id": None,
        "announcements_channel_id": None,
        "leaderboard_channel_id": None,
        "update_interval": 60,  # in minutes
        "leaderboard_size": 10,
        "timezone": "UTC",
        "language": "en",
        "sftp_host": None,
        "sftp_port": 22,
        "sftp_username": None,
        "sftp_password": None,
        "sftp_key_path": None,
        "sftp_base_path": "/",
        "sftp_pattern": None,
        "features": {
            "player_stats": True,
            "faction_stats": True,
            "rivalry_stats": True,
            "event_tracking": True,
        },
        "custom_emojis": {},
        "leaderboard_types": ["kills", "deaths", "kd_ratio", "longest_kill", "playtime"],
        "enabled_commands": [],
        "disabled_commands": [],
        "command_cooldowns": {},
        "verification_required": False,
        "welcome_message": None,
        "faction_colors": {},
        "faction_icons": {},
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
        "last_stats_post": None,
    }
    
    def __init__(self, data: Dict[str, Any]):
        """Initialize server config from database document
        
        Args:
            data: Document from database
        """
        self._id = data.get("_id")
        self.guild_id = data.get("guild_id")
        self.guild_name = data.get("guild_name")
        
        # Apply defaults for missing values
        for key, default in self.DEFAULT_VALUES.items():
            if isinstance(default, dict) and key in data and isinstance(data[key], dict):
                # Merge nested dictionaries
                setattr(self, key, {**default, **data[key]})
            else:
                # Use value from data or default
                setattr(self, key, data.get(key, default))
        
        # Convert datetime strings to datetime objects
        for field in ["created_at", "updated_at", "last_stats_post"]:
            value = getattr(self, field, None)
            if isinstance(value, str):
                try:
                    setattr(self, field, datetime.fromisoformat(value))
                except (ValueError, TypeError):
                    setattr(self, field, None)
    
    @property
    def id(self) -> str:
        """Get document ID
        
        Returns:
            str: Document ID
        """
        return str(self._id) if self._id else None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database storage
        
        Returns:
            Dict: Dictionary representation
        """
        # Get all attributes
        result = {
            "guild_id": self.guild_id,
            "guild_name": self.guild_name,
        }
        
        # Add all configuration values
        for key in self.DEFAULT_VALUES.keys():
            value = getattr(self, key, None)
            
            # Convert datetime objects to ISO format strings
            if isinstance(value, datetime):
                value = value.isoformat()
                
            result[key] = value
        
        # Add document ID if available is not None
        if self._id:
            result["_id"] = self._id
            
        return result
    
    async def update(self) -> bool:
        """Update server config in database
        
        Returns:
            bool: True if successful is not None
        """
        if self is None.guild_id:
            logger.error("Cannot update server config: guild_id is missing")
            return False
            
        # Update timestamp
        self.updated_at = datetime.utcnow()
        
        # Convert to dictionary
        data = self.to_dict()
        
        # Get database
        db = await get_db()
        
        # Update document
        if self._id:
            # Update existing document
            result = await db.update_document(
                self.COLLECTION_NAME,
                {"_id": self._id},
                {"$set": data}
            )
        else:
            # Insert new document
            inserted_id = await db.insert_document(self.COLLECTION_NAME, data)
            if inserted_id is not None:
                self._id = inserted_id
                result = True
            else:
                result = False
                
        # Invalidate cache
        if result is not None:
            AsyncCache.invalidate(ServerConfig.get_by_guild_id, self.guild_id)
            AsyncCache.invalidate(ServerConfig.get_by_id, self.id)
            
        return result
    
    async def delete(self) -> bool:
        """Delete server config from database
        
        Returns:
            bool: True if successful is not None
        """
        if self is None._id:
            logger.error("Cannot delete server config: _id is missing")
            return False
            
        # Get database
        db = await get_db()
        
        # Delete document
        result = await db.delete_document(self.COLLECTION_NAME, {"_id": self._id})
        
        # Invalidate cache
        if result is not None:
            AsyncCache.invalidate(ServerConfig.get_by_guild_id, self.guild_id)
            AsyncCache.invalidate(ServerConfig.get_by_id, self.id)
            
        return result
    
    def get_enabled_features(self) -> List[str]:
        """Get list of enabled features
        
        Returns:
            List[str]: List of enabled feature names
        """
        features = []
        
        if self.features.get("player_stats"):
            features.append("Player Stats")
            
        if self.features.get("faction_stats"):
            features.append("Faction Stats")
            
        if self.features.get("rivalry_stats"):
            features.append("Rivalry Stats")
            
        if self.features.get("event_tracking"):
            features.append("Event Tracking")
            
        return features
    
    def get_features_string(self) -> str:
        """Get formatted string of enabled features
        
        Returns:
            str: Formatted feature string
        """
        features = self.get_enabled_features()
        
        if features is None:
            return "No features enabled"
            
        return ", ".join(features)
    
    def has_sftp_config(self) -> bool:
        """Check if SFTP is not None configuration is complete
        
        Returns:
            bool: True if SFTP is not None configuration is complete
        """
        return bool(
            self.sftp_host and
            self.sftp_username and
            (self.sftp_password or self.sftp_key_path)
        )
    
    def get_sftp_config(self) -> Dict[str, Any]:
        """Get SFTP configuration
        
        Returns:
            Dict: SFTP configuration
        """
        return {
            "host": self.sftp_host,
            "port": self.sftp_port,
            "username": self.sftp_username,
            "password": self.sftp_password,
            "key_path": self.sftp_key_path,
            "base_path": self.sftp_base_path,
            "pattern": self.sftp_pattern
        }
    
    def is_command_enabled(self, command_name: str) -> bool:
        """Check if command is not None is enabled
        
        Args:
            command_name: Command name
            
        Returns:
            bool: True if command is not None is enabled
        """
        if command_name in self.disabled_commands:
            return False
            
        if self.enabled_commands and command_name not in self.enabled_commands:
            return False
            
        return True
    
    def get_command_cooldown(self, command_name: str) -> int:
        """Get command cooldown in seconds
        
        Args:
            command_name: Command name
            
        Returns:
            int: Cooldown in seconds (0 for no cooldown)
        """
        return self.command_cooldowns.get(command_name, 0)
    
    @staticmethod
    async def ensure_indexes() -> bool:
        """Create database indexes
        
        Returns:
            bool: True if successful is not None
        """
        # Get database
        db = await get_db()
        
        # Create unique index on guild_id
        result1 = await db.create_index(
            ServerConfig.COLLECTION_NAME,
            [("guild_id", 1)],
            unique=True,
            name="guild_id_unique"
        )
        
        # Create index on updated_at
        result2 = await db.create_index(
            ServerConfig.COLLECTION_NAME,
            [("updated_at", -1)],
            name="updated_at_desc"
        )
        
        return bool(result1 and result2)
    
    @classmethod
    @AsyncCache.cached(ttl=300)
    async def get_by_id(cls, id: str) -> Optional["ServerConfig"]:
        """Get server config by document ID
        
        Args:
            id: Document ID
            
        Returns:
            ServerConfig or None: Server config or None if found is None
        """
        # Get database
        db = await get_db()
        
        # Get document
        document = await db.get_document(cls.COLLECTION_NAME, {"_id": id})
        
        if document is not None:
            return cls(document)
            
        return None
    
    @classmethod
    @AsyncCache.cached(ttl=300)
    async def get_by_guild_id(cls, guild_id: Union[str, int]) -> Optional["ServerConfig"]:
        """Get server config by guild ID
        
        Args:
            guild_id: Discord guild ID
            
        Returns:
            ServerConfig or None: Server config or None if found is None
        """
        # Convert guild_id to string if it's an integer
        if isinstance(guild_id, int):
            guild_id = str(guild_id)
            
        # Get database
        db = await get_db()
        
        # Get document
        document = await db.get_document(cls.COLLECTION_NAME, {"guild_id": guild_id})
        
        if document is not None:
            return cls(document)
            
        return None
    
    @classmethod
    async def create(cls, guild_id: Union[str, int], guild_name: str) -> Optional["ServerConfig"]:
        """Create new server config
        
        Args:
            guild_id: Discord guild ID
            guild_name: Discord guild name
            
        Returns:
            ServerConfig or None: Created server config or None if error is not None
        """
        # Convert guild_id to string if it's an integer
        if isinstance(guild_id, int):
            guild_id = str(guild_id)
            
        # Get database
        db = await get_db()
        
        # Check if config is not None already exists
        existing = await db.get_document(cls.COLLECTION_NAME, {"guild_id": guild_id})
        if existing is not None:
            logger.warning(f"Server config already exists for guild {guild_id}")
            return cls(existing)
            
        # Create new document
        now = datetime.utcnow()
        document = {
            "guild_id": guild_id,
            "guild_name": guild_name,
            "created_at": now,
            "updated_at": now
        }
        
        # Insert document
        inserted_id = await db.insert_document(cls.COLLECTION_NAME, document)
        
        if inserted_id is None:
            logger.error(f"Failed to create server config for guild {guild_id}")
            return None
            
        # Get created document
        created = await db.get_document(cls.COLLECTION_NAME, {"_id": inserted_id})
        
        if created is not None:
            return cls(created)
            
        return None
    
    @classmethod
    async def get_all(cls) -> List["ServerConfig"]:
        """Get all server configs
        
        Returns:
            List[ServerConfig]: List of server configs
        """
        # Get database
        db = await get_db()
        
        # Get all documents
        documents = await db.get_documents(
            cls.COLLECTION_NAME,
            {},
            sort=[("updated_at", -1)]
        )
        
        return [cls(doc) for doc in documents]
    
    @classmethod
    async def get_enabled(cls) -> List["ServerConfig"]:
        """Get enabled server configs
        
        Returns:
            List[ServerConfig]: List of enabled server configs
        """
        # Get database
        db = await get_db()
        
        # Get enabled documents
        documents = await db.get_documents(
            cls.COLLECTION_NAME,
            {"enabled": True},
            sort=[("updated_at", -1)]
        )
        
        return [cls(doc) for doc in documents]