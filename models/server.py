"""
Server model for Tower of Temptation PvP Statistics Bot

This module defines the Server data structure for game servers.
"""
import logging
from datetime import datetime
from typing import Dict, Any, Optional, ClassVar, List

from models.base_model import BaseModel

logger = logging.getLogger(__name__)

class Server(BaseModel):
    """Game server data"""
    collection_name: ClassVar[Optional[str]] = "game_servers"

    # Server status constants
    STATUS_ACTIVE = "active"
    STATUS_INACTIVE = "inactive"
    STATUS_ERROR = "error"
    STATUS_MAINTENANCE = "maintenance"

    def __init__(
        self,
        server_id: Optional[str] = None,
        guild_id: Optional[str] = None,
        name: Optional[str] = None,
        description: Optional[str] = None,
        status: str = STATUS_ACTIVE,
        hostname: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        sftp_host: Optional[str] = None,
        sftp_port: Optional[int] = None,
        sftp_username: Optional[str] = None,
        sftp_password: Optional[str] = None,
        sftp_directory: Optional[str] = None,
        log_directory: Optional[str] = None,
        last_checked: Optional[datetime] = None,
        last_error: Optional[str] = None,
        players_count: int = 0,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
        original_server_id: Optional[str] = None,  # Added parameter for original numeric server ID
        **kwargs
    ):
        self._id = None
        self.server_id = server_id
        self.guild_id = guild_id
        self.name = name
        self.description = description
        self.status = status
        # Store both the direct hostname params and the sftp_ prefixed ones
        # for backward compatibility
        self.hostname = hostname or sftp_host
        self.port = port or sftp_port
        self.username = username or sftp_username
        self.password = password or sftp_password
        self.sftp_host = sftp_host or hostname
        self.sftp_port = sftp_port or port
        self.sftp_username = sftp_username or username
        self.sftp_password = sftp_password or password
        self.sftp_directory = sftp_directory
        self.log_directory = log_directory
        self.last_checked = last_checked
        self.last_error = last_error
        self.players_count = players_count
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()
        self.original_server_id = original_server_id  # Store the original numeric server ID for path construction

        # Add any additional server attributes
        for key, value in kwargs.items():
            if not hasattr(self, key):
                setattr(self, key, value)

    @classmethod
    async def get_by_server_id(cls, db, server_id: str) -> Optional['Server']:
        """Get a server by server_id

        Args:
            db: Database connection
            server_id: Server ID

        Returns:
            Server object or None if found is None
        """
        document = await db.game_servers.find_one({"server_id": server_id})
        return cls.from_document(document) if document is not None else None
        
    async def save(self, db) -> bool:
        """Save server to database
        
        This ensures that all fields are properly saved, including the sftp_enabled flag
        which is needed for proper SFTP operations.
        
        Args:
            db: Database connection
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Always set sftp_enabled flag for servers with SFTP credentials
            if not hasattr(self, 'sftp_enabled') and all([
                hasattr(self, 'sftp_host') and self.sftp_host,
                hasattr(self, 'sftp_username') and self.sftp_username,
                hasattr(self, 'sftp_password') and self.sftp_password
            ]):
                # Set sftp_enabled explicitly if it has required fields
                self.sftp_enabled = True
                logger.info(f"Setting sftp_enabled=True for server {self.server_id} with valid SFTP credentials")
            
            # Update the updated_at timestamp
            self.updated_at = datetime.utcnow()
            
            # Get document representation
            doc = self.to_document()
            
            # Ensure MongoDB _id field is excluded from the document if it's None
            if '_id' in doc and doc['_id'] is None:
                del doc['_id']
            
            # Check if document already exists
            existing = await db.game_servers.find_one({"server_id": self.server_id})
            
            if existing:
                # Update existing document
                result = await db.game_servers.update_one(
                    {"server_id": self.server_id},
                    {"$set": doc}
                )
                success = result.modified_count > 0
            else:
                # Insert new document
                result = await db.game_servers.insert_one(doc)
                self._id = result.inserted_id
                success = True
                
            # If successful, also update the server in the 'servers' collection for the CSV processor
            # This ensures both collections have the correct SFTP data
            if success:
                # Also update/insert in the servers collection which is used by CSV processor
                servers_result = await db.servers.update_one(
                    {"server_id": self.server_id},
                    {"$set": doc},
                    upsert=True
                )
                logger.info(f"Updated server in servers collection: {servers_result.modified_count} modified, {servers_result.upserted_id != None} upserted")
            
            return success
        except Exception as e:
            logger.error(f"Error saving server {self.server_id}: {e}")
            return False

    @classmethod
    async def get_by_id(cls, db, server_id: str, guild_id: Optional[str] = None) -> Optional['Server']:
        """Get a server by server_id and optionally guild_id
        (This is an alias for get_by_server_id with additional guild_id filter)

        Args:
            db: Database connection
            server_id: Server ID
            guild_id: Optional Guild ID to verify ownership

        Returns:
            Server object or None if not found
        """
        # Import standardize_server_id here to avoid circular imports
        from utils.server_utils import standardize_server_id
        
        # Standardize the server_id to ensure consistent formatting
        standardized_server_id = standardize_server_id(server_id)
        
        if not standardized_server_id:
            logger.warning(f"Invalid server_id format: {server_id}")
            return None
            
        # Build the query with standardized server ID
        query = {"server_id": standardized_server_id}
        if guild_id is not None:
            # Ensure consistent string comparison for guild ID too
            query["guild_id"] = str(guild_id)
        
        # First try exact match
        document = await db.game_servers.find_one(query)
        
        # If no results, try case-insensitive search
        if not document:
            logger.debug(f"No exact match for server_id: {standardized_server_id}, trying case-insensitive search")
            # Create a new case-insensitive search query
            # MongoDB regex pattern for case-insensitive matching
            regex_query = query.copy()
            # We need to modify the dictionary as a whole to avoid type issues
            new_query = {
                **regex_query,
                "server_id": {"$regex": f"^{standardized_server_id}$", "$options": "i"}
            }
            document = await db.game_servers.find_one(new_query)
            
        # If still no results and guild_id is provided, look in the guild's servers list as fallback
        if not document and guild_id is not None:
            logger.debug(f"Server not found in game_servers, checking guild's server list")
            guild_doc = await db.guilds.find_one({"guild_id": str(guild_id)})
            
            if guild_doc and "servers" in guild_doc:
                for server in guild_doc.get("servers", []):
                    # Standardize server ID from guild.servers for comparison
                    server_id_in_guild = standardize_server_id(server.get("server_id"))
                    
                    # Check if server IDs match after standardization
                    if server_id_in_guild == standardized_server_id:
                        # Found server in guild.servers, create a server document
                        logger.info(f"Found server {standardized_server_id} in guild.servers but not in game_servers")
                        # Extract the original numeric server ID for path construction
                        original_server_id = server.get("original_server_id")
                        
                        # If original_server_id is not found, try to extract it from server_id
                        if not original_server_id:
                            # Check if server_id contains numeric segment that could be original ID
                            if server.get("server_id") and any(char.isdigit() for char in server.get("server_id", "")):
                                # Extract numeric part of server ID as fallback for original ID
                                numeric_parts = ''.join(filter(str.isdigit, server.get("server_id", "")))
                                if numeric_parts:
                                    original_server_id = numeric_parts
                                    logger.info(f"Extracted fallback original_server_id {original_server_id} from server_id {server.get('server_id')}")
                        
                        server_doc = {
                            "server_id": standardized_server_id,
                            "guild_id": str(guild_id),
                            "name": server.get("server_name", "Unknown Server"),
                            "sftp_host": server.get("sftp_host"),
                            "sftp_port": server.get("sftp_port"),
                            "sftp_username": server.get("sftp_username"),
                            "sftp_password": server.get("sftp_password"),
                            "log_path": server.get("log_path", ""),
                            "original_server_id": original_server_id,  # Include original numeric server ID
                            "created_at": datetime.utcnow(),
                            "updated_at": datetime.utcnow()
                        }
                        document = server_doc
                        break
        
        return cls.from_document(document) if document is not None else None

    @classmethod
    async def get_by_name(cls, db, name: str, guild_id: str) -> Optional['Server']:
        """Get a server by name and guild_id

        Args:
            db: Database connection
            name: Server name
            guild_id: Guild ID

        Returns:
            Server object or None if found is None
        """
        document = await db.game_servers.find_one({"name": name, "guild_id": guild_id})
        return cls.from_document(document) if document is not None else None

    @classmethod
    async def get_servers_for_guild(cls, db, guild_id: str) -> List['Server']:
        """Get all servers for a guild

        Args:
            db: Database connection
            guild_id: Guild ID

        Returns:
            List of Server objects
        """
        # String conversion for consistent comparison
        guild_id_str = str(guild_id)

        servers = []
        try:
            # Only check game_servers collection
            game_servers_cursor = db.game_servers.find({"guild_id": guild_id_str})
            async for document in game_servers_cursor:
                server = cls.from_document(document)
                if server:
                    servers.append(server)
        except Exception as e:
            logger.error(f"Error fetching servers from game_servers collection: {e}")

        return servers

    @classmethod
    async def get_first_for_guild(cls, db, guild_id: str) -> Optional['Server']:
        """Get the first server for a guild

        Args:
            db: Database connection
            guild_id: Guild ID

        Returns:
            Server object or None if no servers found
        """
        document = await db.game_servers.find_one({"guild_id": guild_id})
        return cls.from_document(document) if document is not None else None

    async def update_status(self, db, status: str, error_message: Optional[str] = None) -> bool:
        """Update server status

        Args:
            db: Database connection
            status: New status
            error_message: Optional error message

        Returns:
            True if updated successfully, False otherwise
        """
        if status is None or status not in [self.STATUS_ACTIVE, self.STATUS_INACTIVE, self.STATUS_ERROR, self.STATUS_MAINTENANCE]:
            return False

        self.status = status
        self.last_checked = datetime.utcnow()
        self.updated_at = datetime.utcnow()

        if error_message is not None and status == self.STATUS_ERROR:
            self.last_error = error_message

        # Update in database
        update_dict = {
            "status": self.status,
            "last_checked": self.last_checked,
            "updated_at": self.updated_at
        }

        if error_message is not None and status == self.STATUS_ERROR:
            update_dict["last_error"] = self.last_error

        result = await db.game_servers.update_one(
            {"server_id": self.server_id},
            {"$set": update_dict}
        )

        return result.modified_count > 0

    async def update_sftp_credentials(self, db, hostname: str, port: int, username: str, password: str, sftp_directory: str) -> bool:
        """Update SFTP credentials

        Args:
            db: Database connection
            hostname: SFTP hostname
            port: SFTP port
            username: SFTP username
            password: SFTP password
            sftp_directory: SFTP directory

        Returns:
            True if updated successfully, False otherwise
        """
        # Update both the direct and sftp_prefixed attributes
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.sftp_host = hostname
        self.sftp_port = port
        self.sftp_username = username
        self.sftp_password = password
        self.sftp_directory = sftp_directory
        self.updated_at = datetime.utcnow()

        # Update in database
        result = await db.game_servers.update_one(
            {"server_id": self.server_id},
            {"$set": {
                "hostname": self.hostname,
                "port": self.port,
                "username": self.username,
                "password": self.password,
                "sftp_host": self.sftp_host,
                "sftp_port": self.sftp_port,
                "sftp_username": self.sftp_username,
                "sftp_password": self.sftp_password,
                "sftp_directory": self.sftp_directory,
                "updated_at": self.updated_at
            }}
        )

        return result.modified_count > 0

    async def update_log_directory(self, db, log_directory: str) -> bool:
        """Update log directory

        Args:
            db: Database connection
            log_directory: Log directory

        Returns:
            True if updated successfully, False otherwise
        """
        self.log_directory = log_directory
        self.updated_at = datetime.utcnow()

        # Update in database
        result = await db.game_servers.update_one(
            {"server_id": self.server_id},
            {"$set": {
                "log_directory": self.log_directory,
                "updated_at": self.updated_at
            }}
        )

        return result.modified_count > 0

    async def update(self, db, update_data: dict) -> bool:
        """General update method for any server attributes

        Args:
            db: Database connection
            update_data: Dictionary of attributes to update

        Returns:
            True if updated successfully, False otherwise
        """
        # Update object attributes
        for key, value in update_data.items():
            if hasattr(self, key):
                setattr(self, key, value)

        # Add updated timestamp
        self.updated_at = datetime.utcnow()
        update_data["updated_at"] = self.updated_at

        # Update in database
        result = await db.game_servers.update_one(
            {"server_id": self.server_id},
            {"$set": update_data}
        )

        return result.modified_count > 0

    @classmethod
    async def create_server(
        cls, 
        db, 
        guild_id: str,
        name: str,
        description: Optional[str] = None,
        hostname: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        sftp_host: Optional[str] = None,
        sftp_port: Optional[int] = None,
        sftp_username: Optional[str] = None,
        sftp_password: Optional[str] = None,
        sftp_directory: Optional[str] = None,
        log_directory: Optional[str] = None,
        original_server_id: Optional[str] = None
    ) -> Optional['Server']:
        """Create a new server

        Args:
            db: Database connection
            guild_id: Guild ID
            name: Server name
            description: Server description
            hostname: SFTP hostname (legacy parameter)
            port: SFTP port (legacy parameter)
            username: SFTP username (legacy parameter)
            password: SFTP password (legacy parameter)
            sftp_host: SFTP hostname (preferred)
            sftp_port: SFTP port (preferred)
            sftp_username: SFTP username (preferred)
            sftp_password: SFTP password (preferred)
            sftp_directory: SFTP directory
            log_directory: Log directory
            original_server_id: Original numeric server ID used for path construction

        Returns:
            Server object or None if creation failed
        """
        import uuid
        
        # Create server ID
        server_id = str(uuid.uuid4())
        
        # IMPORTANT: Extract numeric ID from server name if not provided
        from utils.server_identity import identify_server
        
        if not original_server_id:
            logger.info(f"No original_server_id provided, attempting to extract from name: '{name}'")
            # Try to extract numeric ID from server name or construct a useful one
            for word in str(name).split():
                # Look for numeric parts that are at least 4 digits
                if word.isdigit() and len(word) >= 4:
                    original_server_id = word
                    logger.info(f"Found numeric ID in server name: {original_server_id}")
                    break
                    
            # If we still don't have an original_server_id, ask server_identity module
            if not original_server_id:
                numeric_id, is_known = identify_server(
                    server_id=server_id,
                    hostname=hostname or sftp_host,
                    server_name=name,
                    guild_id=guild_id
                )
                
                if numeric_id:
                    original_server_id = numeric_id
                    logger.info(f"Using identified numeric ID '{numeric_id}' for path construction")
        
        # If we still don't have a numeric ID, create one from last 4 digits of UUID
        if not original_server_id:
            # Extract the last 4-5 digits of the UUID as a fallback numeric ID
            uuid_digits = ''.join(filter(str.isdigit, server_id))
            original_server_id = uuid_digits[-5:] if len(uuid_digits) >= 5 else uuid_digits
            logger.warning(f"No numeric ID found, using extracted digits from UUID: {original_server_id}")

        # Check if server is not None with this name already exists for this guild
        existing_server = await cls.get_by_name(db, name, guild_id)
        if existing_server is not None:
            logger.error(f"Server with name {name} already exists for guild {guild_id}")
            return None

        # Use sftp_ prefixed attributes if available, fall back to legacy names
        final_host = sftp_host or hostname
        final_port = sftp_port or port
        final_username = sftp_username or username
        final_password = sftp_password or password

        # Create server object
        now = datetime.utcnow()
        server = cls(
            server_id=server_id,
            guild_id=guild_id,
            name=name,
            description=description,
            status=cls.STATUS_INACTIVE,  # Start as inactive until verified
            hostname=final_host,
            port=final_port,
            username=final_username,
            password=final_password,
            sftp_host=final_host,
            sftp_port=final_port,
            sftp_username=final_username,
            sftp_password=final_password,
            sftp_directory=sftp_directory,
            log_directory=log_directory,
            last_checked=now,
            players_count=0,
            created_at=now,
            updated_at=now,
            original_server_id=original_server_id  # Store the original numeric server ID
        )
        
        # Log server creation with both IDs for diagnostic purposes
        logger.info(f"Creating server with UUID: {server_id} and original ID: {original_server_id}")

        # Insert into database
        try:
            await db.game_servers.insert_one(server.to_document())
            return server
        except Exception as e:
            logger.error(f"Error creating server: {e}")
            return None

    async def delete(self, db):
        """Delete the server from all collections in a coordinated way"""
        # Initialize counters with safe defaults
        game_count = 0
        standalone_count = 0
        guild_count = 0
        success = False
        
        try:
            # Use standardize_server_id for consistent handling
            from utils.server_utils import standardize_server_id
            std_server_id = standardize_server_id(self.server_id)
            str_server_id = str(self.server_id)  # Keep original for comparison
            str_guild_id = str(self.guild_id) if self.guild_id else None

            logger.info(f"Starting coordinated deletion of server {str_server_id} (std: {std_server_id}) from guild {str_guild_id}")

            # Log server object details for debugging
            logger.info(f"Server object details:")
            logger.info(f"  - server_id: {self.server_id}, type: {type(self.server_id)}")
            logger.info(f"  - guild_id: {self.guild_id}, type: {type(self.guild_id)}")
            logger.info(f"  - name: {self.name}")
            
            # First try to find server in all collections using multiple approaches
            # This helps identify what ID format the server might be stored with
            game_exact = await db.game_servers.find_one({"server_id": str_server_id})
            game_std = await db.game_servers.find_one({"server_id": std_server_id})
            
            standalone_exact = await db.servers.find_one({"server_id": str_server_id})
            standalone_std = await db.servers.find_one({"server_id": std_server_id})
            
            # Check guild using multiple approaches
            guild_exact = await db.guilds.find_one({"servers.server_id": str_server_id})
            guild_std = await db.guilds.find_one({"servers.server_id": std_server_id})
            
            # Try numeric match if ID is numeric
            game_numeric = None
            standalone_numeric = None
            guild_numeric = None
            if std_server_id.isdigit():
                numeric_id = int(std_server_id)
                game_numeric = await db.game_servers.find_one({"server_id": numeric_id})
                standalone_numeric = await db.servers.find_one({"server_id": numeric_id})
                guild_numeric = await db.guilds.find_one({"servers.server_id": numeric_id})
            
            # Log all search results for debugging
            logger.info(f"Server search results:")
            logger.info(f"  - game_servers: exact={game_exact is not None}, std={game_std is not None}, numeric={game_numeric is not None}")
            logger.info(f"  - servers: exact={standalone_exact is not None}, std={standalone_std is not None}, numeric={standalone_numeric is not None}")
            logger.info(f"  - guilds: exact={guild_exact is not None}, std={guild_std is not None}, numeric={guild_numeric is not None}")
            
            # Verify server exists in any collection using any ID format
            if not any([game_exact, game_std, game_numeric, 
                      standalone_exact, standalone_std, standalone_numeric,
                      guild_exact, guild_std, guild_numeric]):
                logger.warning(f"Server {str_server_id} not found in any collection using any ID format")
                return False

            success = False
            try:
                # 1. Remove from game_servers using all possible ID formats
                # This ensures we catch the server regardless of ID format
                game_exact_result = await db.game_servers.delete_many({"server_id": str_server_id})
                game_std_result = await db.game_servers.delete_many({"server_id": std_server_id})
                
                # Numeric match if ID is numeric
                game_numeric_result = None
                if std_server_id.isdigit():
                    numeric_id = int(std_server_id)
                    game_numeric_result = await db.game_servers.delete_many({"server_id": numeric_id})
                
                # Case-insensitive regex match as last resort
                game_regex_result = await db.game_servers.delete_many({
                    "server_id": {"$regex": f"^{std_server_id}$", "$options": "i"}
                })
                
                # Calculate total deleted
                game_count = (
                    game_exact_result.deleted_count + 
                    game_std_result.deleted_count + 
                    (game_numeric_result.deleted_count if game_numeric_result else 0) +
                    game_regex_result.deleted_count
                )
                logger.info(f"Removed {game_count} entries from game_servers - " +
                           f"Exact: {game_exact_result.deleted_count}, " +
                           f"Std: {game_std_result.deleted_count}, " +
                           f"Numeric: {game_numeric_result.deleted_count if game_numeric_result else 0}, " +
                           f"Regex: {game_regex_result.deleted_count}")

                # 2. Remove from standalone servers using same approach
                standalone_exact_result = await db.servers.delete_many({"server_id": str_server_id})
                standalone_std_result = await db.servers.delete_many({"server_id": std_server_id})
                
                # Numeric match if ID is numeric
                standalone_numeric_result = None
                if std_server_id.isdigit():
                    numeric_id = int(std_server_id)
                    standalone_numeric_result = await db.servers.delete_many({"server_id": numeric_id})
                
                # Case-insensitive regex match as last resort
                standalone_regex_result = await db.servers.delete_many({
                    "server_id": {"$regex": f"^{std_server_id}$", "$options": "i"}
                })
                
                # Calculate total deleted
                standalone_count = (
                    standalone_exact_result.deleted_count + 
                    standalone_std_result.deleted_count +
                    (standalone_numeric_result.deleted_count if standalone_numeric_result else 0) +
                    standalone_regex_result.deleted_count
                )
                logger.info(f"Removed {standalone_count} standalone server entries - " +
                           f"Exact: {standalone_exact_result.deleted_count}, " +
                           f"Std: {standalone_std_result.deleted_count}, " +
                           f"Numeric: {standalone_numeric_result.deleted_count if standalone_numeric_result else 0}, " +
                           f"Regex: {standalone_regex_result.deleted_count}")

                # 3. Remove from all guilds' server arrays
                guild_result = await db.guilds.update_many(
                    {"servers.server_id": str_server_id},
                    {
                        "$pull": {"servers": {"server_id": str_server_id}},
                        "$set": {"updated_at": datetime.utcnow()}
                    }
                )
                guild_count = guild_result.modified_count  # Update our counter
                logger.info(f"Updated {guild_count} guilds")

                # Always consider successful if we found and removed from any collection
                success = (game_count > 0 or standalone_count > 0 or guild_count > 0)

                # Perform atomic integration cleanup
                try:
                    # Start a session for atomic operations
                    async with await db.client.start_session() as session:
                        async with session.start_transaction():
                            # Remove all integration-related records
                            collections_to_clean = [
                                "integrations",
                                "integration_configs", 
                                "integration_settings",
                                "integration_status",
                                "integration_logs",
                                "integration_metrics"
                            ]

                            for collection in collections_to_clean:
                                try:
                                    await db[collection].delete_many(
                                        {"server_id": str_server_id},
                                        session=session
                                    )
                                except Exception as e:
                                    logger.warning(f"Collection {collection} cleanup failed: {e}")
                                    # Continue with other collections

                            # Clean up cross-references
                            await db.server_links.delete_many(
                                {
                                    "$or": [
                                        {"server_id": str_server_id},
                                        {"linked_server_id": str_server_id}
                                    ]
                                },
                                session=session
                            )

                            # Mark any remaining integration references as inactive
                            await db.server_integrations.update_many(
                                {"server_id": str_server_id},
                                {
                                    "$set": {
                                        "status": "inactive",
                                        "updated_at": datetime.utcnow(),
                                        "deactivated_at": datetime.utcnow()
                                    }
                                },
                                session=session
                            )

                            logger.info(f"Successfully cleaned up all integration records for server {str_server_id}")

                except Exception as e:
                    logger.error(f"Critical error during integration cleanup for server {str_server_id}: {e}")
                    raise RuntimeError(f"Integration cleanup failed: {str(e)}")

            except Exception as e:
                logger.error(f"Error during server deletion: {e}")
                success = False
                
                # Fall back to trying direct cleanup of player records and integrations
                fallback_success = False
                try:
                    # 4. Clean up any related collections and integrations
                    await db.players.update_many(
                        {"server_id": str_server_id},
                        {"$set": {"active": False, "updated_at": datetime.utcnow()}}
                    )

                    # Remove from integration collections
                    await db.integrations.delete_many({"server_id": str_server_id})
                    await db.integration_configs.delete_many({"server_id": str_server_id})
                    await db.integration_settings.delete_many({"server_id": str_server_id})

                    # Remove from cross-reference collections
                    await db.server_links.delete_many({
                        "$or": [
                            {"server_id": str_server_id},
                            {"linked_server_id": str_server_id}
                        ]
                    })

                    # Clean up any integration status records
                    await db.integration_status.delete_many({"server_id": str_server_id})
                    
                    fallback_success = True
                    logger.info(f"Fallback cleanup for server {str_server_id} succeeded")
                except Exception as fallback_error:
                    logger.error(f"Error during fallback cleanup for server {str_server_id}: {fallback_error}")
                    # Continue with deletion even if fallback cleanup fails
                
                # If the fallback succeeded but the main deletion failed, still consider it successful
                if fallback_success:
                    success = True

            # Update counts in main try block instead - moved to after the db operations
            # We'll keep our safely initialized defaults from the start of the method
            
            logger.info(
                f"Server {str_server_id} deletion completed:\n"
                f"- Game servers removed: {game_count}\n"
                f"- Standalone removed: {standalone_count}\n"
                f"- Guilds updated: {guild_count}"
            )

            return success

        except Exception as e:
            logger.error(f"Error in coordinated deletion of server {self.server_id}: {e}")
            return False