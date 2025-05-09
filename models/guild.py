"""
Guild model for Tower of Temptation PvP Statistics Bot

This module defines the Guild data structure for Discord guilds.
"""
import logging
from datetime import datetime
from typing import Dict, Any, Optional, ClassVar, List, Union, Tuple, cast
import uuid

from models.base_model import BaseModel

logger = logging.getLogger(__name__)

class Guild(BaseModel):
    """Discord guild configuration"""
    collection_name: ClassVar[Optional[str]] = "guilds"

    def to_dict(self) -> Dict[str, Any]:
        """Convert Guild object to dictionary
        
        Returns:
            Dict containing all guild data
        """
        return {
            "_id": self._id,
            "guild_id": self.guild_id,
            "name": self.name,
            "premium_tier": self.premium_tier,
            "admin_role_id": self.admin_role_id,
            "admin_users": self.admin_users,
            "servers": self.servers,
            "color_primary": self.color_primary,
            "color_secondary": self.color_secondary, 
            "color_accent": self.color_accent,
            "icon_url": self.icon_url,
            "created_at": self.created_at,
            "updated_at": self.updated_at
        }

    def __init__(
        self,
        db,
        guild_id: Optional[str] = None,
        name: Optional[str] = None,
        premium_tier: int = 0,
        admin_role_id: Optional[str] = None,
        admin_users: Optional[List[str]] = None,
        servers: Optional[List[Dict[str, Any]]] = None,
        color_primary: str = "#7289DA",
        color_secondary: str = "#FFFFFF",
        color_accent: str = "#23272A",
        icon_url: Optional[str] = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
        **kwargs
    ):
        self._id = None
        self.db = db
        self.guild_id = guild_id
        self.name = name
        self.premium_tier = premium_tier
        self.admin_role_id = admin_role_id
        self.admin_users = admin_users or []
        self.color_primary = color_primary
        self.color_secondary = color_secondary
        self.color_accent = color_accent
        self.icon_url = icon_url
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()

        self.servers = servers or []

        # Add any additional guild attributes
        for key, value in kwargs.items():
            if not hasattr(self, key):
                setattr(self, key, value)

    async def add_server(self, server_data: Dict[str, Any]) -> bool:
        """Add a server to the guild

        Args:
            server_data: Server configuration dictionary

        Returns:
            bool: True if added successfully, False otherwise
        """
        if server_data is None or server_data.get("server_id") is None or server_data.get("server_id") == "":
            return False
            
        # CRITICAL FIX: Ensure original_server_id is ALWAYS a numeric ID for path construction
        # This is the most important part of the solution to the UUID path issue
        server_id = server_data.get("server_id")
        original_server_id = server_data.get("original_server_id")
        hostname = server_data.get("hostname", server_data.get("sftp_host", ""))
        server_name = server_data.get("server_name", "")
        
        # Import server_identity for consistent ID resolution
        try:
            from utils.server_identity import identify_server, KNOWN_SERVERS
            
            # 1. Priority 1: Check KNOWN_SERVERS for both server_id and original_server_id
            if server_id in KNOWN_SERVERS:
                numeric_id = KNOWN_SERVERS[server_id]
                logger.info(f"Using known numeric ID '{numeric_id}' from KNOWN_SERVERS for server {server_id}")
                server_data["original_server_id"] = numeric_id
            elif original_server_id and original_server_id in KNOWN_SERVERS:
                numeric_id = KNOWN_SERVERS[original_server_id]
                logger.info(f"Using known numeric ID '{numeric_id}' from KNOWN_SERVERS for original_server_id {original_server_id}")
                server_data["original_server_id"] = numeric_id
                
            # 2. Priority 2: If original_server_id is provided and is numeric, use it directly
            elif original_server_id and str(original_server_id).isdigit():
                logger.info(f"Using existing numeric original_server_id: {original_server_id}")
                # Keep existing value - already set
                
            # 3. Priority 3: Extract from server name (often contains numeric IDs)
            elif server_name:
                # Look for numeric ID in server name (common pattern: "Server 7020" or "EU 7020")
                found = False
                for word in str(server_name).split():
                    if word.isdigit() and len(word) >= 4:
                        logger.info(f"Found numeric ID in server name: {word}")
                        server_data["original_server_id"] = word
                        found = True
                        break
                        
                # 4. Priority 4: Try server_identity resolution if nothing found in server name
                if not found:
                    numeric_id, is_known = identify_server(
                        server_id=server_id,
                        hostname=hostname,
                        server_name=server_name,
                        guild_id=self.guild_id
                    )
                    
                    if numeric_id:
                        logger.info(f"Using identified numeric ID '{numeric_id}' from server_identity module")
                        server_data["original_server_id"] = numeric_id
                        found = True
                        
                # 5. Priority 5: Extract digits from server_id as last resort
                if not found and server_id:
                    # Get numeric part of UUID if possible
                    uuid_digits = ''.join(filter(str.isdigit, str(server_id)))
                    if uuid_digits:
                        extracted_id = uuid_digits[-5:] if len(uuid_digits) >= 5 else uuid_digits
                        logger.warning(f"Using extracted digits from server_id: {extracted_id}")
                        server_data["original_server_id"] = extracted_id
                    else:
                        # Absolute last resort: Generate a random numeric ID
                        import random
                        fallback_id = str(random.randint(10000, 99999))
                        logger.error(f"Could not determine any numeric ID, using random fallback: {fallback_id}")
                        server_data["original_server_id"] = fallback_id
            
            # 6. Catch-all for any case where we still don't have an original_server_id
            if "original_server_id" not in server_data or not server_data["original_server_id"]:
                # Last attempt with server_identity
                numeric_id, is_known = identify_server(
                    server_id=server_id,
                    hostname=hostname,
                    server_name="", 
                    guild_id=self.guild_id
                )
                
                if numeric_id:
                    logger.info(f"Final attempt: using identified numeric ID '{numeric_id}'")
                    server_data["original_server_id"] = numeric_id
                else:
                    # Absolute final fallback
                    import random
                    fallback_id = str(random.randint(10000, 99999))
                    logger.error(f"No valid numeric ID could be found, using random fallback: {fallback_id}")
                    server_data["original_server_id"] = fallback_id
                    
        except ImportError as e:
            logger.error(f"Failed to import server_identity, using basic fallback: {e}")
            # Simple fallback if server_identity can't be imported
            if not original_server_id:
                # Extract from server name
                for word in str(server_name).split():
                    if word.isdigit() and len(word) >= 4:
                        server_data["original_server_id"] = word
                        break
                else:
                    # Use server_id digits as fallback
                    if server_id:
                        uuid_digits = ''.join(filter(str.isdigit, str(server_id)))
                        extracted_id = uuid_digits[-5:] if len(uuid_digits) >= 5 else uuid_digits
                        server_data["original_server_id"] = extracted_id or server_id
                
        logger.info(f"Adding server with server_id={server_data.get('server_id')} and original_server_id={server_data.get('original_server_id')}")

        # Add server to list
        self.servers.append(server_data)
        self.updated_at = datetime.utcnow()

        # Update in database
        result = await self.db.guilds.update_one(
            {"guild_id": self.guild_id},
            {
                "$set": {
                    "servers": self.servers,
                    "updated_at": self.updated_at
                }
            }
        )
        
        # IMPORTANT: Also save to servers collection for CSV processor
        # This ensures the server is found by the historical parser
        try:
            # Make sure sftp_enabled is set for servers with SFTP credentials
            if all(key in server_data for key in ["sftp_host", "sftp_username", "sftp_password"]):
                server_data["sftp_enabled"] = True
                
            # Save to servers collection (used by CSV processor)
            server_result = await self.db.servers.update_one(
                {"server_id": server_data["server_id"]},
                {"$set": server_data},
                upsert=True  # Create if doesn't exist
            )
            logger.info(f"Added server to 'servers' collection: {server_data['server_id']}, upsert={server_result.upserted_id is not None}")
            
            # Also save to game_servers collection for better compatibility
            game_server_result = await self.db.game_servers.update_one(
                {"server_id": server_data["server_id"]},
                {"$set": server_data},
                upsert=True
            )
            logger.info(f"Added server to 'game_servers' collection: {server_data['server_id']}, upsert={game_server_result.upserted_id is not None}")
        except Exception as e:
            logger.error(f"Error saving server to collections: {e}")

        return result.modified_count > 0

    async def remove_server(self, server_id: Union[str, int, None]) -> bool:
        """Remove a server from the guild and standalone collection

        Args:
            server_id: Server ID to remove (string, int, or other convertible type)

        Returns:
            bool: True if removed successfully, False otherwise
        """
        # Ensure we have database instance
        if not hasattr(self, 'db') or not self.db:
            # If no database connection, use application-level removal only
            logger.warning("No database connection available for Guild.remove_server")
            
            # Import standardize_server_id here to avoid circular imports
            from utils.server_utils import standardize_server_id
            
            # Standardize the server_id to ensure consistent formatting
            standardized_server_id = standardize_server_id(server_id)
            
            # Memory-only removal from servers array
            if hasattr(self, 'servers') and self.servers:
                before_count = len(self.servers)
                self.servers = [s for s in self.servers if standardize_server_id(s.get("server_id")) != standardized_server_id]
                after_count = len(self.servers)
                return before_count > after_count
            
            return False

        # Import standardize_server_id here to avoid circular imports
        from utils.server_utils import standardize_server_id
        
        # Standardize the server_id to ensure consistent formatting
        standardized_server_id = standardize_server_id(server_id)
        str_server_id = str(server_id) if server_id is not None else ""
        
        if not standardized_server_id:
            logger.warning(f"Invalid server_id format for removal: {server_id}")
            return False
            
        # Log server information before removal for debugging
        logger.info(f"Removing server with ID from guild {self.guild_id}:")
        logger.info(f"  - Raw server_id: {server_id}, Type: {type(server_id)}")
        logger.info(f"  - String repr: {str_server_id}")
        logger.info(f"  - Standardized: {standardized_server_id}")
        
        # Make sure servers is initialized
        if not hasattr(self, 'servers') or self.servers is None:
            self.servers = []
            
        # Log all existing servers for debugging
        logger.info(f"Current servers in guild {self.guild_id}:")
        for i, s in enumerate(self.servers):
            s_id = s.get("server_id")
            s_name = s.get("server_name", "Unknown")
            std_id = standardize_server_id(s_id)
            logger.info(f"  - Server {i}: ID={s_id}, StdID={std_id}, Name={s_name}, Type={type(s_id)}")
        
        # First try exact match
        found_exact = False
        for s in self.servers:
            if s.get("server_id") == standardized_server_id:
                found_exact = True
                logger.info(f"Found exact match for server ID {standardized_server_id}")
                break
                
        # Find and remove server from guild.servers, using standardized comparison
        original_server_count = len(self.servers)
        
        # Try different matching approaches if needed
        if found_exact:
            # Use exact matching first
            self.servers = [s for s in self.servers if s.get("server_id") != standardized_server_id]
        else:
            # Try case-insensitive standardized matching
            logger.info(f"No exact match found, using standardized comparison")
            self.servers = [s for s in self.servers if standardize_server_id(s.get("server_id")) != standardized_server_id]
            
            # Also try direct string comparison with raw server_id
            if len(self.servers) == original_server_count:
                logger.info(f"No matches with standardized comparison, trying raw string comparison")
                self.servers = [s for s in self.servers if str(s.get("server_id")) != str_server_id]
            
            # Last resort: try numeric comparison if server_id is numeric
            if standardized_server_id.isdigit() and len(self.servers) == original_server_count:
                logger.info(f"No matches with string comparison, trying numeric comparison")
                numeric_id = int(standardized_server_id)
                self.servers = [s for s in self.servers if (
                    not str(s.get("server_id")).isdigit() or int(s.get("server_id")) != numeric_id
                )]
        
        servers_removed = original_server_count - len(self.servers)
        
        if servers_removed > 0:
            logger.info(f"Removed {servers_removed} server entries from guild.servers array")
        else:
            logger.warning(f"No servers matched {standardized_server_id} in guild.servers array")
            
        self.updated_at = datetime.utcnow()

        # Update guild in database
        guild_result = await self.db.guilds.update_one(
            {"guild_id": self.guild_id},
            {
                "$set": {
                    "servers": self.servers,
                    "updated_at": self.updated_at
                }
            }
        )
        
        # Try multiple approaches to remove from standalone servers collection
        # 1. First try exact match
        standalone_exact = await self.db.servers.delete_many({"server_id": standardized_server_id})
        
        # 2. Try case-insensitive regex match
        standalone_query = {"server_id": {"$regex": f"^{standardized_server_id}$", "$options": "i"}}
        standalone_result = await self.db.servers.delete_many(standalone_query)
        
        # 3. If server ID is numeric, try numeric match
        standalone_numeric = 0
        if standardized_server_id.isdigit():
            try:
                numeric_id = int(standardized_server_id)
                numeric_result = await self.db.servers.delete_many({"server_id": numeric_id})
                standalone_numeric = numeric_result.deleted_count
            except:
                pass
                
        # Similar approaches for game_servers collection
        # 1. Exact match
        game_exact = await self.db.game_servers.delete_many({"server_id": standardized_server_id})
        
        # 2. Case-insensitive match
        game_servers_query = {"server_id": {"$regex": f"^{standardized_server_id}$", "$options": "i"}}
        game_regex = await self.db.game_servers.delete_many(game_servers_query)
        
        # 3. Numeric match if applicable
        game_numeric = 0
        if standardized_server_id.isdigit():
            try:
                numeric_id = int(standardized_server_id)
                numeric_result = await self.db.game_servers.delete_many({"server_id": numeric_id})
                game_numeric = numeric_result.deleted_count
            except:
                pass
        
        # Combine all deletion counts
        standalone_count = standalone_exact.deleted_count + standalone_result.deleted_count + standalone_numeric
        game_count = game_exact.deleted_count + game_regex.deleted_count + game_numeric
        
        # Log detailed deletion results
        logger.info(f"Server removal results - Guild: {guild_result.modified_count}")
        logger.info(f"Servers collection - Exact: {standalone_exact.deleted_count}, Regex: {standalone_result.deleted_count}, Numeric: {standalone_numeric}")
        logger.info(f"Game servers - Exact: {game_exact.deleted_count}, Regex: {game_regex.deleted_count}, Numeric: {game_numeric}")

        return guild_result.modified_count > 0 or standalone_count > 0 or game_count > 0

    async def get_server(self, server_id: Union[str, int, None]) -> Optional[Dict[str, Any]]:
        """Get a server by ID
        
        This method ensures server_id is always treated as a string
        for consistent comparisons, following the standardized approach.

        Args:
            server_id: Server ID to find (will be converted to string)

        Returns:
            Optional[Dict]: Server data if found, None otherwise
        """
        # Use the standardize_server_id function from server_utils
        # for consistent handling across the codebase
        from utils.server_utils import standardize_server_id
        
        # Standardize server ID
        str_server_id = standardize_server_id(server_id)
        if str_server_id is None or str_server_id == "":
            logger.warning(f"Invalid server_id provided to get_server: {server_id}")
            return None
        
        # Check servers list exists and is actually a list
        if not hasattr(self, 'servers') or not isinstance(self.servers, list):
            logger.warning(f"Guild {self.guild_id} has no servers attribute or it is not a list")
            return None
            
        # Check each server with consistent string conversion
        for server in self.servers:
            if not isinstance(server, dict):
                logger.warning(f"Non-dict server entry found in guild {self.guild_id}: {type(server)}")
                continue
                
            # Get server_id with fallback to empty string if missing
            server_id_value = server.get("server_id", "")
            if server_id_value is None:
                continue
                
            # Always compare as strings with consistent handling
            if standardize_server_id(server_id_value) == str_server_id:
                return server
                
        return None

    def get_max_servers(self) -> int:
        """Get maximum number of servers allowed for guild's tier"""
        from config import PREMIUM_TIERS
        tier_info = PREMIUM_TIERS.get(self.premium_tier, {})
        return tier_info.get("max_servers", 1)

    @classmethod
    async def get_by_guild_id(cls, db, guild_id: str) -> Optional['Guild']:
        """Get a guild by guild_id

        Args:
            db: Database connection
            guild_id: Discord guild ID (will be converted to string)

        Returns:
            Guild object or None if not found
        """
        # Always ensure guild_id is a string for consistent lookup
        string_guild_id = str(guild_id) if guild_id is not None else None
        
        if string_guild_id is None:
            logger.warning("Attempted to get guild with None guild_id")
            return None
            
        document = await db.guilds.find_one({"guild_id": string_guild_id})
        return cls.create_from_db_document(document, db) if document is not None else None

    async def set_premium_tier(self, db, tier: int) -> bool:
        """Set premium tier for guild

        Args:
            db: Database connection
            tier: Premium tier (0-4)

        Returns:
            True if updated successfully, False otherwise
        """
        # Validate tier range
        if tier < 0 or tier > 4:
            logger.warning(f"Attempt to set invalid premium tier: {tier}")
            return False

        # Ensure tier is an integer
        try:
            tier_int = int(tier)
        except (ValueError, TypeError):
            logger.error(f"Invalid tier type: {type(tier).__name__}, value: {tier}")
            return False

        # Log the tier change
        logger.info(f"Setting premium tier for guild {self.guild_id}: {self.premium_tier} -> {tier_int}")
            
        # Set tier in model
        self.premium_tier = tier_int
        self.updated_at = datetime.utcnow()

        # Update in database
        try:
            result = await db.guilds.update_one(
                {"guild_id": self.guild_id},
                {"$set": {
                    "premium_tier": tier_int,  # Explicitly use tier_int for db storage
                    "updated_at": self.updated_at
                }}
            )
            
            success = result.modified_count > 0
            if success:
                logger.info(f"Successfully updated premium tier for guild {self.guild_id} to {tier_int}")
            else:
                logger.warning(f"Failed to update premium tier for guild {self.guild_id}, no documents modified")
                
            return success
        except Exception as e:
            logger.error(f"Error updating premium tier: {e}")
            return False

    async def set_admin_role(self, db, role_id: str) -> bool:
        """Set admin role for guild

        Args:
            db: Database connection
            role_id: Discord role ID

        Returns:
            True if updated successfully, False otherwise
        """
        self.admin_role_id = role_id
        self.updated_at = datetime.utcnow()

        # Update in database
        result = await db.guilds.update_one(
            {"guild_id": self.guild_id},
            {"$set": {
                "admin_role_id": self.admin_role_id,
                "updated_at": self.updated_at
            }}
        )

        return result.modified_count > 0

    async def add_admin_user(self, db, user_id: str) -> bool:
        """Add admin user for guild

        Args:
            db: Database connection
            user_id: Discord user ID

        Returns:
            True if updated successfully, False otherwise
        """
        if not hasattr(self, "admin_users"):
            self.admin_users = []

        if user_id in self.admin_users:
            return True

        self.admin_users.append(user_id)
        self.updated_at = datetime.utcnow()

        # Update in database
        result = await db.guilds.update_one(
            {"guild_id": self.guild_id},
            {"$set": {
                "admin_users": self.admin_users,
                "updated_at": self.updated_at
            }}
        )

        return result.modified_count > 0

    async def remove_admin_user(self, db, user_id: str) -> bool:
        """Remove admin user for guild

        Args:
            db: Database connection
            user_id: Discord user ID

        Returns:
            True if updated successfully, False otherwise
        """
        if not hasattr(self, "admin_users") or user_id not in self.admin_users:
            return True

        self.admin_users.remove(user_id)
        self.updated_at = datetime.utcnow()

        # Update in database
        result = await db.guilds.update_one(
            {"guild_id": self.guild_id},
            {"$set": {
                "admin_users": self.admin_users,
                "updated_at": self.updated_at
            }}
        )

        return result.modified_count > 0

    async def update_theme(self, db, color_primary: Optional[str] = None, color_secondary: Optional[str] = None, color_accent: Optional[str] = None, icon_url: Optional[str] = None) -> bool:
        """Update theme colors for guild

        Args:
            db: Database connection
            color_primary: Primary color (hex)
            color_secondary: Secondary color (hex)
            color_accent: Accent color (hex)
            icon_url: Icon URL

        Returns:
            True if updated successfully, False otherwise
        """
        # Timestamp for the update
        update_timestamp = datetime.utcnow()
        
        # Create the update dictionary with only the fields to update
        # This avoids including color fields that might have non-datetime values
        update_dict = {}
        update_dict["updated_at"] = update_timestamp

        # Only add fields that are explicitly being updated
        if color_primary is not None:
            self.color_primary = color_primary
            update_dict["color_primary"] = color_primary

        if color_secondary is not None:
            self.color_secondary = color_secondary
            update_dict["color_secondary"] = color_secondary

        if color_accent is not None:
            self.color_accent = color_accent
            update_dict["color_accent"] = color_accent

        if icon_url is not None:
            self.icon_url = icon_url
            update_dict["icon_url"] = icon_url

        # Update the instance timestamp
        self.updated_at = update_timestamp

        # Update in database
        result = await db.guilds.update_one(
            {"guild_id": self.guild_id},
            {"$set": update_dict}
        )

        return result.modified_count > 0

    @classmethod
    async def get_by_id(cls, db, guild_id) -> Optional['Guild']:
        """Get a guild by its Discord ID (alias for get_by_guild_id)
        
        Args:
            db: Database connection
            guild_id: Discord guild ID (will be converted to string)
            
        Returns:
            Guild object or None if not found
        """
        # This method is an alias that ensures backward compatibility
        # While ensuring the same type safety as get_by_guild_id
        return await cls.get_by_guild_id(db, guild_id)
        
    @classmethod
    async def get_guild(cls, db, guild_id) -> Optional['Guild']:
        """Get a guild by its Discord ID (alias for get_by_id)
        
        This method is used throughout the codebase for consistency.
        
        Args:
            db: Database connection
            guild_id: Discord guild ID (will be converted to string)
            
        Returns:
            Guild object or None if not found
        """
        return await cls.get_by_id(db, guild_id)
        
    @classmethod
    async def get_or_create(cls, db, guild_id, guild_name: Optional[str] = None) -> Optional['Guild']:
        """Get an existing guild or create a new one if it doesn\'t exist
        
        This is particularly useful for premium validation where we might need
        a guild model without requiring complete setup.
        
        Args:
            db: Database connection 
            guild_id: Discord guild ID (will be converted to string)
            guild_name: Optional guild name to use if creating a new guild (defaults to "Guild {guild_id}")
            
        Returns:
            Guild object or None if retrieval/creation failed
        """
        # First try to get the existing guild
        guild = await cls.get_by_id(db, guild_id)
        
        # If guild exists, return it directly
        if guild is not None:
            return guild
            
        # Otherwise create a new guild with basic information
        if guild_name is None:
            guild_name = f"Guild {guild_id}"
            
        # Create the guild with minimal setup
        try:
            logger.info(f"Auto-creating guild during feature access: {guild_id}")
            return await cls.create(db, guild_id, guild_name)
        except Exception as e:
            logger.error(f"Error auto-creating guild during feature access: {e}")
            return None

    @classmethod
    async def create(cls, db, guild_id: str, name: str) -> Optional['Guild']:
        """Create a new guild

        Args:
            db: Database connection
            guild_id: Discord guild ID
            name: Guild name

        Returns:
            Created Guild object or None if creation failed
        """
        # Create document
        document = {
            "guild_id": str(guild_id),
            "name": name,
            "premium_tier": 0,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }

        # Insert into database
        try:
            result = await db.guilds.insert_one(document)
            if result.inserted_id is not None:
                document["_id"] = result.inserted_id
                return cls.create_from_db_document(document, db)
        except Exception as e:
            logger.error(f"Error creating guild: {e}")

        return None

    def check_feature_access(self, feature_name: str) -> bool:
        """Check if this guild has access to a premium feature
        
        This comprehensive method implements proper tier inheritance, ensuring that
        higher tiers have access to all features from lower tiers. It checks
        feature access using multiple methods to ensure reliability.

        Args:
            feature_name: Name of the feature to check

        Returns:
            True if the guild has access to the feature, False otherwise
        """
        from config import PREMIUM_TIERS
        from utils.premium import PREMIUM_FEATURES

        # Log for debugging purposes
        logger.info(f"Checking feature access for '{feature_name}' in guild with tier {self.premium_tier}")
        
        # Make sure premium_tier is an integer (fix for potential string storage issue)
        try:
            premium_tier = int(self.premium_tier) if self.premium_tier is not None else 0
        except (ValueError, TypeError):
            logger.warning(f"Invalid premium_tier value: {self.premium_tier}, defaulting to 0")
            premium_tier = 0
        
        # Method 1: Check using direct PREMIUM_FEATURES mapping (most efficient)
        # This implements tier inheritance by checking if current tier >= required tier
        if feature_name in PREMIUM_FEATURES:
            min_tier_required = PREMIUM_FEATURES.get(feature_name, 999)  # Default to high tier if not found
            has_access = premium_tier >= min_tier_required
            logger.info(f"Feature check via PREMIUM_FEATURES: feature '{feature_name}' requires tier {min_tier_required}, guild has tier {premium_tier}, access: {has_access}")
            if has_access:
                return True
        
        # Method 2: Check using inherited features from all tiers up to and including current tier
        all_features = []
        
        # Gather features from all tiers up to and including the current tier
        # This implements tier inheritance by explicitly collecting features from all lower tiers
        for tier_level in range(premium_tier + 1):
            tier_info = PREMIUM_TIERS.get(tier_level)
            if tier_info:
                tier_features = tier_info.get("features", [])
                for feature in tier_features:
                    if feature not in all_features:
                        all_features.append(feature)
        
        logger.info(f"Available features after tier inheritance (0 to {premium_tier}): {all_features}")
        has_access = feature_name in all_features
        logger.info(f"Guild access to '{feature_name}': {has_access}")
        
        return has_access

    def get_available_features(self) -> List[str]:
        """
        Get list of features available for this guild's premium tier.
        
        This method ensures higher tiers have access to all features from lower tiers,
        implementing a comprehensive tier inheritance system.
        """
        from config import PREMIUM_TIERS
        from utils.premium import PREMIUM_FEATURES
        
        # DEBUG: Add more detailed logging to track premium tier values
        logger.info(f"PREMIUM DEBUG: Raw premium_tier value: '{self.premium_tier}', type: {type(self.premium_tier)}")
        
        # Make sure premium_tier is an integer (fix for potential string storage issue)
        try:
            # First check direct numeric conversion
            if isinstance(self.premium_tier, (int, float)):
                premium_tier = int(self.premium_tier)
            # Then try string conversion if it's a string
            elif isinstance(self.premium_tier, str) and self.premium_tier.strip().isdigit():
                premium_tier = int(self.premium_tier.strip())
            # Handle None case
            elif self.premium_tier is None:
                premium_tier = 0
                logger.warning("Premium tier is None, defaulting to 0")
            else:
                # Last resort, try direct conversion
                premium_tier = int(self.premium_tier)
                
            logger.info(f"PREMIUM DEBUG: Converted premium_tier: {premium_tier}")
        except (ValueError, TypeError) as e:
            logger.warning(f"Invalid premium_tier value in get_available_features: {self.premium_tier}, error: {str(e)}, defaulting to 0")
            premium_tier = 0
        
        # Initialize feature list with all features accessible at this tier level
        all_features = []
        
        # Method 1: Collect features from all tiers up to and including this tier
        # This ensures proper tier inheritance - higher tiers include all features from lower tiers
        for tier_level in range(premium_tier + 1):
            tier_info = PREMIUM_TIERS.get(tier_level)
            if tier_info:
                tier_features = tier_info.get("features", [])
                for feature in tier_features:
                    if feature not in all_features:
                        all_features.append(feature)
        
        # If no features were found via tier inheritance, use tier 0 as fallback
        if not all_features:
            logger.warning(f"No tier info found for any tier up to {premium_tier} in get_available_features, using tier 0 as fallback")
            tier_info = PREMIUM_TIERS.get(0, {})
            all_features = tier_info.get("features", [])
        
        logger.info(f"PREMIUM DEBUG: Features from tier inheritance (0 to {premium_tier}): {all_features}")
        
        # Method 2: Supplement with direct feature minimum tier mapping
        # This is a backup approach to ensure all features are included
        supplemental_features = []
        for feature, min_tier in PREMIUM_FEATURES.items():
            if premium_tier >= min_tier:
                # Only add if not already in the list
                if feature not in all_features:
                    supplemental_features.append(feature)
        
        if supplemental_features:
            logger.info(f"PREMIUM DEBUG: Additional features from PREMIUM_FEATURES: {supplemental_features}")
            all_features.extend(supplemental_features)
        
        # Ensure no duplicate features
        unique_features = list(set(all_features))
        logger.info(f"PREMIUM DEBUG: Final feature set for tier {premium_tier}: {unique_features}")
            
        return unique_features

    @classmethod
    def create_from_db_document(cls, document: Dict[str, Any], db=None) -> Optional['Guild']:
        """Create a Guild instance from a database document with db connection
        
        This is a custom method specifically for Guild to handle db parameter.
        Unlike BaseModel.from_document, this accepts a db connection parameter.
        
        Args:
            document: MongoDB document
            db: Database connection
            
        Returns:
            Guild instance or None if document is None
        """
        if document is None:
            return None
        
        # Extract and properly convert premium_tier to int before initializing
        # This ensures premium tier is always stored as an integer
        document_copy = document.copy()
        
        # Handle premium_tier conversion specifically
        if 'premium_tier' in document_copy:
            try:
                # Convert premium_tier to integer
                premium_tier_value = document_copy['premium_tier']
                if premium_tier_value is not None:
                    # Ensure it's an integer
                    document_copy['premium_tier'] = int(premium_tier_value)
                    logger.info(f"Loaded premium_tier from database: {document_copy['premium_tier']} (original type: {type(premium_tier_value).__name__})")
                else:
                    # Default to 0 if None
                    document_copy['premium_tier'] = 0
                    logger.warning("Premium tier is None in database, defaulting to 0")
            except (ValueError, TypeError) as e:
                logger.error(f"Error converting premium_tier '{document_copy['premium_tier']}': {e}, defaulting to 0")
                document_copy['premium_tier'] = 0
        else:
            # If no premium_tier in document, default to 0
            document_copy['premium_tier'] = 0
            logger.warning("No premium_tier found in database document, defaulting to 0")
            
        instance = cls(db, **document_copy)
        
        # Ensure all IDs are strings for consistent handling
        if hasattr(instance, 'guild_id'):
            instance.guild_id = str(instance.guild_id)
        if hasattr(instance, 'admin_role_id') and instance.admin_role_id is not None:
            instance.admin_role_id = str(instance.admin_role_id)
            
        return instance
        
    @classmethod
    def from_document(cls, document: Dict[str, Any]) -> Optional['Guild']:
        """Create a Guild instance from a database document
        
        Overrides BaseModel.from_document to maintain compatibility.
        This method doesn't include a db parameter to match the BaseModel signature.
        
        Args:
            document: MongoDB document
            
        Returns:
            Guild instance or None if document is None (matching BaseModel contract)
        """
        if document is None:
            return None
            
        return cls.create_from_db_document(document, None)