"""
CSV Processor cog for the Tower of Temptation PvP Statistics Discord Bot.

This cog provides:
1. Background task for downloading and processing CSV files from game servers
2. Commands for manually processing CSV files
3. Admin commands for managing CSV processing
"""
import asyncio
import logging
import os
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple

import discord
from discord.ext import commands
# Ensure discord_compat is imported for py-cord compatibility
from utils.discord_compat import get_app_commands_module
app_commands = get_app_commands_module(), tasks
# Use compatibility layer to handle different Discord library versions
from utils.discord_compat import get_app_commands_module, AppCommandOptionType

# Get the appropriate app_commands module for the current Discord library
app_commands = get_app_commands_module()

from utils.csv_parser import CSVParser
from utils.sftp import SFTPManager
from utils.embed_builder import EmbedBuilder
from utils.helpers import has_admin_permission
from utils.parser_utils import parser_coordinator, normalize_event_data, categorize_event
from utils.decorators import has_admin_permission as admin_permission_decorator, premium_tier_required
from models.guild import Guild
from models.server import Server
from utils.discord_utils import server_id_autocomplete  # Import standardized autocomplete function

logger = logging.getLogger(__name__)

class CSVProcessorCog(commands.Cog):
    """Commands and background tasks for processing CSV files"""

    def __init__(self, bot: commands.Bot):
        """Initialize the CSV processor cog

        Args:
            bot: Discord bot instance
        """
        self.bot = bot
        self.csv_parser = CSVParser()
        # Don't initialize SFTP manager here, we'll create instances as needed
        self.sftp_managers = {}  # Store SFTP managers by server_id
        self.processing_lock = asyncio.Lock()
        self.is_processing = False
        self.last_processed = {}  # Track last processed timestamp per server

        # Create task loop
        self.process_csv_files_task = tasks.loop(minutes=5.0)(self.process_csv_files_task)
        # Before loop hook
        self.process_csv_files_task.before_loop(self.before_process_csv_files_task)
        # Start background task
        self.process_csv_files_task.start()

    def cog_unload(self):
        """Stop background tasks and close connections when cog is unloaded"""
        if hasattr(self.process_csv_files_task, 'cancel'):
            self.process_csv_files_task.cancel()

        # Close all SFTP connections
        for server_id, sftp_manager in self.sftp_managers.items():
            try:
                asyncio.create_task(sftp_manager.disconnect())
            except Exception as e:
                logger.error(f"Error disconnecting SFTP for server {server_id}: {e}")

    async def process_csv_files_task(self):
        """Background task for processing CSV files

        This task runs every 5 minutes and checks for new CSV files on all configured servers.
        """
        if self.is_processing:
            logger.debug("Skipping CSV processing - already running")
            return

        self.is_processing = True

        try:
            # Get list of configured servers
            server_configs = await self._get_server_configs()

            # Skip processing if no SFTP-enabled servers are configured
            if not server_configs:
                logger.debug("No SFTP-enabled servers configured, skipping CSV processing")
                return

            for server_id, config in server_configs.items():
                try:
                    await self._process_server_csv_files(server_id, config)
                except Exception as e:
                    logger.error(f"Error processing CSV files for server {server_id}: {str(e)}")

        except Exception as e:
            logger.error(f"Error in CSV processing task: {str(e)}")

        finally:
            self.is_processing = False

    async def before_process_csv_files_task(self):
        """Wait for bot to be ready before starting task"""
        await self.bot.wait_until_ready()
        # Add a small delay to avoid startup issues
        await asyncio.sleep(10)

    async def _get_server_configs(self) -> Dict[str, Dict[str, Any]]:
        """Get configurations for all servers with SFTP enabled

        This method searches through various collections to find server configurations,
        including the standalone 'servers' collection, the 'game_servers' collection,
        and embedded server configurations within guild documents.

        Returns:
            Dict: Dictionary of server IDs to server configurations
        """
        # Query database for server configurations with SFTP enabled
        server_configs = {}

        # Import standardization function
        from utils.server_utils import standardize_server_id

        # Find all servers with SFTP configuration in the database
        try:
            # IMPORTANT: We need to query multiple collections to ensure we find all servers
            logger.debug("Getting server configurations from all collections")

            # Dictionary to track which servers we've already processed (by standardized ID)
            processed_servers = set()

            # 1. First try the primary 'servers' collection
            logger.debug("Checking 'servers' collection for SFTP configurations")
            servers_cursor = self.bot.db.servers.find({
                "$and": [
                    {"sftp_host": {"$exists": True}},
                    {"sftp_username": {"$exists": True}},
                    {"sftp_password": {"$exists": True}}
                ]
            })

            count = 0
            async for server in servers_cursor:
                raw_server_id = server.get("server_id")
                server_id = standardize_server_id(raw_server_id)

                if not server_id:
                    logger.warning(f"Invalid server ID format in servers collection: {raw_server_id}, skipping")
                    continue

                # Process this server
                await self._process_server_config(server, server_id, raw_server_id, server_configs)
                processed_servers.add(server_id)
                count += 1

            logger.debug(f"Found {count} servers with SFTP config in 'servers' collection")

            # 2. Also check the 'game_servers' collection for additional servers
            logger.debug("Checking 'game_servers' collection for SFTP configurations")
            game_servers_cursor = self.bot.db.game_servers.find({
                "$and": [
                    {"sftp_host": {"$exists": True}},
                    {"sftp_username": {"$exists": True}},
                    {"sftp_password": {"$exists": True}}
                ]
            })

            game_count = 0
            async for server in game_servers_cursor:
                raw_server_id = server.get("server_id")
                server_id = standardize_server_id(raw_server_id)

                if not server_id:
                    logger.warning(f"Invalid server ID format in game_servers collection: {raw_server_id}, skipping")
                    continue

                # Skip if we've already processed this server
                if server_id in processed_servers:
                    logger.debug(f"Server {server_id} already processed from 'servers' collection, skipping duplicate")
                    continue

                # Process this server
                await self._process_server_config(server, server_id, raw_server_id, server_configs)
                processed_servers.add(server_id)
                game_count += 1

            logger.debug(f"Found {game_count} additional servers with SFTP config in 'game_servers' collection")

            # 3. Check for embedded server configurations in guild documents
            logger.debug("Checking for embedded server configurations in guilds collection")
            guilds_cursor = self.bot.db.guilds.find({})

            guild_count = 0
            guild_server_count = 0
            async for guild in guilds_cursor:
                guild_count += 1
                guild_id = guild.get("guild_id")
                guild_servers = guild.get("servers", [])

                if not guild_servers:
                    continue

                for server in guild_servers:
                    # Skip if not a dictionary
                    if not isinstance(server, dict):
                        continue

                    raw_server_id = server.get("server_id")
                    server_id = standardize_server_id(raw_server_id)

                    if not server_id:
                        continue

                    # Skip if we've already processed this server
                    if server_id in processed_servers:
                        continue

                    # Only consider servers with SFTP configuration
                    if all(key in server for key in ["sftp_host", "sftp_username", "sftp_password"]):
                        # Add the guild_id to the server config
                        server["guild_id"] = guild_id

                        # Process this server
                        await self._process_server_config(server, server_id, raw_server_id, server_configs)
                        processed_servers.add(server_id)
                        guild_server_count += 1

            logger.info(f"Found {guild_server_count} additional servers with SFTP config in {guild_count} guilds")

            # Final log of all server configurations found
            logger.info(f"Total servers with SFTP config: {len(server_configs)}")
            if server_configs:
                logger.info(f"Server IDs found: {list(server_configs.keys())}")

        except Exception as e:
            logger.error(f"Error retrieving server configurations: {e}")

        return server_configs

    async def _process_server_config(self, server: Dict[str, Any], server_id: str, 
                                   raw_server_id: str, server_configs: Dict[str, Dict[str, Any]]) -> None:
        """Process a server configuration and add it to the server_configs dictionary

        Args:
            server: Server document from database
            server_id: Standardized server ID
            raw_server_id: Original server ID from database
            server_configs: Dictionary to add the processed config to
        """
        try:
            # Log the original and standardized server IDs for debugging
            logger.debug(f"Processing server: original={raw_server_id}, standardized={server_id}")

            # Only add servers with complete SFTP configuration
            if all(key in server for key in ["sftp_host", "sftp_username", "sftp_password"]):
                # The sftp_host might include the port in format "hostname:port"
                sftp_host = server.get("sftp_host")
                sftp_port = server.get("sftp_port", 22)  # Default to 22 if not specified

                # Split hostname and port if they're combined
                if sftp_host and ":" in sftp_host:
                    hostname_parts = sftp_host.split(":")
                    sftp_host = hostname_parts[0]  # Extract just the hostname part
                    if len(hostname_parts) > 1 and hostname_parts[1].isdigit():
                        sftp_port = int(hostname_parts[1])  # Use the port from the combined string

                # Get the original_server_id from the document if available,
                # otherwise use the raw_server_id passed to this method
                original_server_id = server.get("original_server_id", raw_server_id)
                if not original_server_id:
                    original_server_id = raw_server_id

                # Log the original server ID being used
                logger.debug(f"Using original_server_id={original_server_id} for server {server_id}")

                server_configs[server_id] = {
                    # Map database parameter names to what SFTPManager expects
                    "hostname": sftp_host,
                    "port": int(sftp_port),
                    "username": server.get("sftp_username"),
                    "password": server.get("sftp_password"),
                    # Keep additional parameters with original names
                    "sftp_path": server.get("sftp_path", "/logs"),
                    "csv_pattern": server.get("csv_pattern", r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}\.csv"),
                    # Use the properly determined original_server_id for path construction
                    "original_server_id": original_server_id,
                    # Store the guild_id if available
                    "guild_id": server.get("guild_id")
                }
                logger.debug(f"Added configured SFTP server: {server_id}")
        except Exception as e:
            logger.error(f"Error processing server config for {server_id}: {e}")

    # This method is no longer used, replaced by the more comprehensive _get_server_configs method
    # The functionality has been migrated to _get_server_configs and _process_server_config

    async def _process_server_csv_files(self, server_id: str, config: Dict[str, Any]) -> Tuple[int, int]:
        """Process CSV files for a specific server

        Args:
            server_id: Server ID
            config: Server configuration

        Returns:
            Tuple[int, int]: Number of files processed and total death events processed
        """
        # Connect to SFTP server - use the correctly mapped parameters
        hostname = config["hostname"]  # Already mapped in _get_server_configs
        port = config["port"]          # Already mapped in _get_server_configs
        username = config["username"]  # Already mapped in _get_server_configs
        password = config["password"]  # Already mapped in _get_server_configs

        # Get last processed time or default to 24 hours ago
        last_time = self.last_processed.get(server_id, datetime.now() - timedelta(days=1))

        # Format for SFTP directory listing comparison
        last_time_str = last_time.strftime("%Y.%m.%d-%H.%M.%S")

        # Initialize return values
        files_processed = 0
        events_processed = 0
        
        # Initialize the main try-except-finally structure
        try:
            # Create a new SFTP client for this server if none exists
            if server_id not in self.sftp_managers:
                logger.debug(f"Creating new SFTP manager for server {server_id}")
                # Create SFTPManager with the correct parameter mapping
                # Get original_server_id if it exists, otherwise use server_id
                original_server_id = config.get("original_server_id")

                # If we have no original_server_id but the server_id looks like a UUID,
                # let's try to extract a numeric ID from the server name or other properties if available
                if not original_server_id and "-" in server_id and len(server_id) > 30:
                    logger.debug(f"Server ID appears to be in UUID format: {server_id}")
                    logger.debug(f"Checking for numeric server ID in server properties")

                    # Try to find a numeric ID in server name (which is often in format "Server 7020")
                    server_name = config.get("server_name", "")
                    if server_name:
                        # Try to extract a numeric ID from the server name
                        for word in str(server_name).split():
                            if word.isdigit() and len(word) >= 4:
                                logger.debug(f"Found potential numeric server ID in server_name: {word}")
                                original_server_id = word
                                break

                if original_server_id:
                    logger.debug(f"Using original server ID for path construction: {original_server_id}")
                else:
                    logger.debug(f"No original numeric server ID found, using UUID for path construction: {server_id}")
                    original_server_id = server_id

                self.sftp_managers[server_id] = SFTPManager(
                    hostname=hostname,  # Map from sftp_host above
                    port=port,          # Map from sftp_port
                    username=username,  # Map from sftp_username
                    password=password,  # Map from sftp_password
                    server_id=server_id,  # Pass server_id for tracking
                    original_server_id=original_server_id  # Pass original server ID for path construction
                )

            # Get the SFTP client for this server
            sftp = self.sftp_managers[server_id]

            # Check if there was a recent connection error
            if hasattr(sftp, 'last_error') and sftp.last_error and 'Auth failed' in sftp.last_error:
                logger.warning(f"Skipping SFTP operations for server {server_id} due to recent authentication failure")
                return 0, 0

            # Track connection state
            was_connected = sftp.client is not None
            logger.debug(f"SFTP connection state before connect: connected={was_connected}")

            # Connect or ensure connection is active
            if not was_connected:
                await sftp.connect()

            try:
                # Get the configured SFTP path from server settings
                sftp_path = config.get("sftp_path", "/logs")

                # Always use original_server_id for path construction
                # Always try to get original_server_id first
                path_server_id = config.get("original_server_id")

                # If no original_server_id, try numeric extraction from hostname
                if not path_server_id:
                    hostname = config.get("hostname", "")
                    if "_" in hostname:
                        potential_id = hostname.split("_")[-1]
                        if potential_id.isdigit():
                            path_server_id = potential_id
                            logger.info(f"Using numeric ID from hostname: {potential_id}")

                # Second attempt: try server name
                if not path_server_id:
                    server_name = config.get("server_name", "")
                    for word in str(server_name).split():
                        if word.isdigit() and len(word) >= 4:
                            path_server_id = word
                            logger.info(f"Using numeric ID from server name: {word}")
                            break

                # Last resort: use server_id but log warning
                if not path_server_id:
                    logger.warning(f"No numeric ID found, using server_id as fallback: {server_id}")
                    path_server_id = server_id

                # Build server directory using the determined path_server_id
                server_dir = f"{config.get('hostname', 'server').split(':')[0]}_{path_server_id}"
                logger.info(f"Using server directory: {server_dir} with ID {path_server_id}")
                logger.debug(f"Using server directory: {server_dir}")

                # Initialize variables to avoid "possibly unbound" warnings
                alternate_deathlogs_paths = []
                csv_files = []
                path_found = None

                # Build CSV file paths
                if sftp_path and sftp_path.startswith("/"):
                    # Use configured absolute path
                    deathlogs_path = sftp_path
                    logger.debug(f"Using configured absolute path: {deathlogs_path}")
                else:
                    # Use default path structure
                    deathlogs_path = os.path.join("/", server_dir, "actual1", "deathlogs")
                    logger.debug(f"Using default path structure: {deathlogs_path}")

                # Define standard paths to check
                standard_paths = [
                    deathlogs_path,  # Primary path
                    os.path.join(deathlogs_path, "world_0"),  # Map directories
                    os.path.join(deathlogs_path, "world_1"),
                    os.path.join(deathlogs_path, "world_2"),
                    os.path.join(deathlogs_path, "world_3"),
                    os.path.join(deathlogs_path, "world_4"),
                    os.path.join("/", server_dir, "deathlogs"),  # Alternate locations
                    os.path.join("/", server_dir, "logs"),
                    os.path.join("/", "logs", server_dir)
                ]
                logger.debug(f"Will check {len(standard_paths)} standard paths")

                # Get CSV pattern from config - ensure it will correctly match CSV files with dates
                csv_pattern = config.get("csv_pattern", r".*\.csv$")
                # Add fallback patterns specifically for date-formatted CSV files with multiple format support
                # Handle both pre-April and post-April CSV format timestamp patterns
                date_format_patterns = [
                    # Primary pattern - Tower of Temptation uses YYYY.MM.DD-HH.MM.SS.csv format
                    r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}\.csv$",  # YYYY.MM.DD-HH.MM.SS.csv (primary format)

                    # Common year-first date formats
                    r"\d{4}\.\d{2}\.\d{2}.*\.csv$",                    # YYYY.MM.DD*.csv (any time format)
                    r"\d{4}-\d{2}-\d{2}.*\.csv$",                      # YYYY-MM-DD*.csv (ISO date format)

                    # Day-first formats (less common but possible)
                    r"\d{2}\.\d{2}\.\d{4}.*\.csv$",                    # DD.MM.YYYY*.csv (European format)

                    # Most flexible pattern to catch any date-like format
                    r"\d{2,4}[.-_]\d{1,2}[.-_]\d{1,4}.*\.csv$",        # Any date-like pattern

                    # Ultimate fallback - any CSV file as absolute last resort
                    r".*\.csv$"
                ]
                # Use the first pattern as primary fallback
                date_format_pattern = date_format_patterns[0]

                logger.debug(f"Using primary CSV pattern: {csv_pattern}")
                logger.debug(f"Using date format patterns: {date_format_patterns}")

                # Log which patterns we're using to find CSV files
                logger.debug(f"Looking for CSV files with primary pattern: {csv_pattern}")
                logger.debug(f"Fallback pattern for date-formatted files: {date_format_pattern}")


                # First check: Are there map subdirectories in the deathlogs path?
                try:
                    # Verify deathlogs_path exists
                    if await sftp.exists(deathlogs_path):
                        logger.debug(f"Deathlogs path exists: {deathlogs_path}, checking for map subdirectories")

                        # Define known map directory names to check directly (maps we know exist)
                        known_map_names = ["world_0", "world0", "world_1", "world1", "map_0", "map0", "main", "default"]
                        logger.debug(f"Checking for these known map directories first: {known_map_names}")

                        # Try to directly check known map directories first
                        map_directories = []
                        for map_name in known_map_names:
                            map_path = os.path.join(deathlogs_path, map_name)
                            logger.debug(f"Directly checking for map directory: {map_path}")

                            try:
                                if await sftp.exists(map_path):
                                    logger.debug(f"Found known map directory: {map_path}")
                                    map_directories.append(map_path)
                            except Exception as map_err:
                                logger.debug(f"Error checking known map directory {map_path}: {map_err}")

                        # If we didn't find any known map directories, list all directories in deathlogs
                        if not map_directories:
                            logger.debug("No known map directories found, checking all directories in deathlogs")
                            try:
                                deathlogs_entries = await sftp.client.listdir(deathlogs_path)
                                logger.debug(f"Found {len(deathlogs_entries)} entries in deathlogs directory")

                                # Find all subdirectories (any directory under deathlogs could be a map)
                                for entry in deathlogs_entries:
                                    if entry in ('.', '..'):
                                        continue

                                    entry_path = os.path.join(deathlogs_path, entry)
                                    try:
                                        entry_info = await sftp.get_file_info(entry_path)
                                        if entry_info and entry_info.get("is_dir", False):
                                            logger.debug(f"Found potential map directory: {entry_path}")
                                            map_directories.append(entry_path)
                                    except Exception as entry_err:
                                        logger.debug(f"Error checking entry {entry_path}: {entry_err}")
                            except Exception as list_err:
                                logger.warning(f"Error listing deathlogs directory: {list_err}")

                        logger.debug(f"Found {len(map_directories)} total map directories")

                        # If we found map directories, search each one for CSV files
                        if map_directories:
                            all_map_csv_files = []

                            for map_dir in map_directories:
                                try:
                                    # Look for CSV files in this map directory
                                    map_csv_files = await sftp.list_files(map_dir, csv_pattern)

                                    if map_csv_files:
                                        logger.info(f"Found {len(map_csv_files)} CSV files in map directory {map_dir}")
                                        # Convert to full paths
                                        map_full_paths = [
                                            os.path.join(map_dir, f) for f in map_csv_files
                                            if not f.startswith('/')  # Only relative paths need joining
                                        ]
                                        all_map_csv_files.extend(map_full_paths)
                                    else:
                                        # Try with each date format pattern
                                        for pattern in date_format_patterns:
                                            logger.debug(f"Trying pattern {pattern} in map directory {map_dir}")
                                            date_map_csv_files = await sftp.list_files(map_dir, pattern)
                                            if date_map_csv_files:
                                                logger.info(f"Found {len(date_map_csv_files)} CSV files using pattern {pattern} in map directory {map_dir}")
                                                # Convert to full paths
                                                map_full_paths = [
                                                    os.path.join(map_dir, f) for f in date_map_csv_files
                                                    if not f.startswith('/')
                                                ]
                                                all_map_csv_files.extend(map_full_paths)
                                                break  # Stop after finding files with one pattern

                                        # Log if no files were found with any pattern
                                        found_any = False
                                        for pattern in date_format_patterns:
                                            if await sftp.list_files(map_dir, pattern):
                                                found_any = True
                                                break

                                        if not found_any:
                                            logger.debug(f"No CSV files found with any pattern in map directory {map_dir}")
                                except Exception as map_err:
                                    logger.warning(f"Error searching map directory {map_dir}: {map_err}")

                            # If we found CSV files in any map directory
                            if all_map_csv_files:
                                logger.info(f"Found {len(all_map_csv_files)} total CSV files across all map directories")
                                full_path_csv_files = all_map_csv_files
                                csv_files = [os.path.basename(f) for f in all_map_csv_files]
                                path_found = deathlogs_path  # Use the parent deathlogs path as the base

                                # Log a sample of found files
                                if len(csv_files) > 0:
                                    sample = csv_files[:5] if len(csv_files) > 5 else csv_files
                                    logger.info(f"Sample CSV files: {sample}")
                    else:
                        logger.warning(f"Deathlogs path does not exist: {deathlogs_path}")
                except Exception as e:
                    logger.warning(f"Error checking for map directories: {e}")

                # If we already found files in map directories, we can skip the rest of the search
                if csv_files:
                    logger.info(f"Successfully found CSV files in map directories, skipping standard search")
                else:
                    logger.info(f"No CSV files found in map directories, continuing with standard search")

                # Enhanced list of possible paths to check (when map directories search fails)
                # For Tower of Temptation, we need to include possible map subdirectory paths

                # Define known map subdirectory names
                map_subdirs = ["world_0", "world0", "world_1", "world1", "map_0", "map0", "main", "default"]

                # Build base paths list
                base_paths = [
                    deathlogs_path,  # Standard path: /hostname_serverid/actual1/deathlogs/
                    os.path.join("/", server_dir, "deathlogs"),  # Without "actual1"
                    os.path.join("/", server_dir, "logs"),  # Alternate logs directory
                    os.path.join("/", server_dir, "Logs", "deathlogs"),  # Capital Logs with deathlogs subdirectory
                    os.path.join("/", server_dir, "Logs"),  # Just capital Logs
                    os.path.join("/", "logs", server_dir),  # Common format with server subfolder
                    os.path.join("/", "deathlogs"),  # Root deathlogs 
                    os.path.join("/", "logs"),  # Root logs
                    os.path.join("/", server_dir),  # Just server directory
                    os.path.join("/", server_dir, "actual1"),  # Just the actual1 directory
                ]

                # Now add map subdirectory variations to each base path
                possible_paths = []
                for base_path in base_paths:
                    # Add the base path first
                    possible_paths.append(base_path)

                    # Then add each map subdirectory variation
                    for map_subdir in map_subdirs:
                        map_path = os.path.join(base_path, map_subdir)
                        possible_paths.append(map_path)

                # Add root as last resort
                possible_paths.append("/")

                logger.debug(f"Generated {len(possible_paths)} possible paths to search for CSV files")

                # First attempt: Use list_files with the specified pattern on all possible paths
                for search_path in possible_paths:
                    logger.debug(f"Trying to list CSV files in: {search_path}")
                    try:
                        # Check connection before each attempt
                        if not sftp.client:
                            logger.warning(f"Connection lost before listing files in {search_path}, reconnecting...")
                            await sftp.connect()
                            if not sftp.client:
                                logger.error(f"Failed to reconnect for path: {search_path}")
                                continue

                        # Try with primary pattern
                        path_files = await sftp.list_files(search_path, csv_pattern)

                        # If primary pattern didn't work, try with each date format pattern
                        if not path_files and csv_pattern != date_format_pattern:
                            logger.debug(f"No files found with primary pattern, trying date format patterns in {search_path}")
                            for pattern in date_format_patterns:
                                logger.debug(f"Trying pattern {pattern} in directory {search_path}")
                                pattern_files = await sftp.list_files(search_path, pattern)
                                if pattern_files:
                                    logger.info(f"Found {len(pattern_files)} CSV files using pattern {pattern} in {search_path}")
                                    path_files = pattern_files
                                    break

                        if path_files:
                            # Build full paths to the CSV files
                            full_paths = [
                                f if f.startswith('/') else os.path.join(search_path, f) 
                                for f in path_files
                            ]

                            # Check which are actually files (not directories)
                            verified_files = []
                            verified_full_paths = []

                            for i, file_path in enumerate(full_paths):
                                try:
                                    if await sftp.is_file(file_path):
                                        verified_files.append(path_files[i])
                                        verified_full_paths.append(file_path)
                                except Exception as verify_err:
                                    logger.warning(f"Error verifying file {file_path}: {verify_err}")

                            if verified_files:
                                csv_files = verified_files
                                full_path_csv_files = verified_full_paths
                                path_found = search_path
                                logger.info(f"Found {len(csv_files)} CSV files in {search_path}")

                                # Print the first few file names for debugging
                                if csv_files:
                                    sample_files = csv_files[:5]
                                    logger.info(f"Sample CSV files: {sample_files}")

                                break
                    except Exception as path_err:
                        logger.warning(f"Error listing files in {search_path}: {path_err}")
                        # Continue to next path

                    # Second attempt: Try recursive search immediately with more paths and deeper search
                    if not csv_files:
                        logger.info(f"No CSV files found in predefined paths, trying recursive search...")

                        # Try first from server root, then the root directory of the server
                        root_paths = [
                            server_dir,  # Server's root directory
                            "/",         # File system root
                            os.path.dirname(server_dir) if "/" in server_dir else "/",  # Parent of server dir
                            os.path.join("/", "data"),  # Common server data directory
                            os.path.join("/", "game"),  # Game installation directory
                            # More specific paths
                            os.path.join("/", server_dir, "game"),
                            os.path.join("/", "home", os.path.basename(server_dir) if server_dir != "/" else "server"),
                            os.path.join("/", "home", "steam", os.path.basename(server_dir) if server_dir != "/" else "server"),
                            os.path.join("/", "game", os.path.basename(server_dir) if server_dir != "/" else "server"),
                            os.path.join("/", "data", os.path.basename(server_dir) if server_dir != "/" else "server"),
                        ]

                        logger.debug(f"Will try recursive search from {len(root_paths)} different root paths")

                        for root_path in root_paths:
                            try:
                                # Check connection before recursive search
                                if not sftp.client:
                                    logger.warning(f"Connection lost before recursive search at {root_path}, reconnecting...")
                                    await sftp.connect()
                                    if not sftp.client:
                                        logger.error(f"Failed to reconnect for recursive search at {root_path}")
                                        continue

                                logger.debug(f"Starting deep recursive search from {root_path}")

                                # Use find_csv_files which has better error handling and multiple fallbacks
                                if hasattr(sftp, 'find_csv_files'):
                                    # Try with higher max_depth to explore deeper into the file structure
                                    root_csvs = await sftp.find_csv_files(root_path, recursive=True, max_depth=8)
                                    if root_csvs:
                                        logger.info(f"Found {len(root_csvs)} CSV files in deep search from {root_path}")
                                        # Log a sample of the files found
                                        if len(root_csvs) > 0:
                                            sample = root_csvs[:5] if len(root_csvs) > 5 else root_csvs
                                            logger.info(f"Sample files: {sample}")

                                        # Filter for CSV files that match our pattern
                                        pattern_re = re.compile(csv_pattern)
                                        matching_csvs = [
                                            f for f in root_csvs
                                            if pattern_re.search(os.path.basename(f))
                                        ]

                                        # If no matches with primary pattern, try date format pattern
                                        if not matching_csvs and csv_pattern != date_format_pattern:
                                            logger.debug(f"No matches with primary pattern, trying date format pattern")
                                            pattern_re = re.compile(date_format_pattern)
                                            matching_csvs = [
                                                f for f in root_csvs
                                                if pattern_re.search(os.path.basename(f))
                                            ]

                                            if matching_csvs:
                                                # Found matching CSV files
                                                full_path_csv_files = matching_csvs
                                                csv_files = [os.path.basename(f) for f in matching_csvs]
                                                path_found = os.path.dirname(matching_csvs[0])
                                                logger.info(f"Found {len(csv_files)} CSV files through recursive search in {path_found}")

                                                # Print the first few file names for debugging
                                                if csv_files:
                                                    sample_files = csv_files[:5]
                                                    logger.info(f"Sample CSV files: {sample_files}")

                                                break

                                    # If we found files, break out of the root_path loop
                                    if csv_files:
                                        break

                            except Exception as search_err:
                                logger.warning(f"Recursive CSV search failed for {root_path}: {search_err}")

                    # Third attempt: Last resort - manually search common directories with simpler method
                    if not csv_files:
                        logger.info(f"Still no CSV files found, trying direct file stat checks...")
                        # This is a last resort method to check for CSV files
                        # by directly trying to stat specific paths with clear date patterns

                        # Generate some likely filenames with date patterns
                        current_time = datetime.now()
                        test_dates = [
                            current_time - timedelta(days=i)
                            for i in range(0, 31, 5)  # Try dates at 5-day intervals going back a month
                        ]

                        test_filenames = []
                        for test_date in test_dates:
                            # Format: YYYY.MM.DD-00.00.00.csv (daily file at midnight)
                            test_filenames.append(test_date.strftime("%Y.%m.%d-00.00.00.csv"))
                            # Also try hourly files from the most recent day
                            if test_date == test_dates[0]:
                                for hour in range(0, 24, 6):  # Try every 6 hours
                                    test_filenames.append(test_date.strftime(f"%Y.%m.%d-{hour:02d}.00.00.csv"))

                        # Try these filenames in each potential directory
                        for search_path in possible_paths:
                            if csv_files:  # Break early if we found something
                                break

                            for filename in test_filenames:
                                test_path = os.path.join(search_path, filename)
                                try:
                                    # Try to stat the file directly
                                    if await sftp.exists(test_path):
                                        logger.info(f"Found CSV file using direct check: {test_path}")
                                        # We found one file, now search the directory for more
                                        path_files = await sftp.list_files(search_path, r".*\.csv$")
                                        if path_files:
                                            csv_files = path_files
                                            path_found = search_path
                                            full_path_csv_files = [os.path.join(search_path, f) for f in csv_files]
                                            logger.info(f"Found {len(csv_files)} CSV files in {search_path} using direct check")
                                            break
                                except Exception as direct_err:
                                    pass  # Silently continue, we're trying lots of paths

                        # If we still have no files or path, return empty results
                        if not csv_files or path_found is None:
                            logger.warning(f"No CSV files found for server {server_id} after exhaustive search")
                            return 0, 0

                        # Update deathlogs_path with the path where we actually found files (guaranteed to be non-None at this point)
                        deathlogs_path = path_found  # path_found is definitely not None here

                        # Sort chronologically
                        csv_files.sort()

                        # Filter for files newer than last processed
                        new_files = [f for f in csv_files if f > last_time_str]

                        # Log what we found
                        logger.info(f"Found {len(new_files)} new CSV files out of {len(csv_files)} total in {deathlogs_path}")

                        # Process each file
                        files_processed = 0
                        events_processed = 0

                        for file in new_files:
                            try:
                                # Download file content - use the correct path
                                file_path = os.path.join(deathlogs_path, file)
                                logger.debug(f"Downloading CSV file from: {file_path}")
                                content = await sftp.download_file(file_path)

                                if content:
                                    # Process content
                                    events = self.csv_parser.parse_csv_data(content.decode('utf-8'))

                                    # Normalize and deduplicate events
                                    processed_count = 0
                                    errors = []

                                    for event in events:
                                        try:
                                            # Normalize event data
                                            normalized_event = normalize_event_data(event)

                                            # Add server ID
                                            normalized_event["server_id"] = server_id

                                            # Check if this is a duplicate event
                                            if parser_coordinator.is_duplicate_event(normalized_event):
                                                # Update timestamp in coordinator
                                                if "timestamp" in normalized_event and isinstance(normalized_event["timestamp"], datetime):
                                                    parser_coordinator.update_csv_timestamp(server_id, normalized_event["timestamp"])

                                                # Process kill event based on type
                                                event_type = categorize_event(normalized_event)

                                                if event_type in ["kill", "suicide"]:
                                                    # Process kill event
                                                    await self._process_kill_event(normalized_event)
                                                    processed_count += 1

                                        except Exception as e:
                                            errors.append(str(e))

                                    processed = processed_count

                                    events_processed += processed
                                    files_processed += 1

                                    if errors:
                                        logger.warning(f"Errors processing {file}: {len(errors)} errors")

                                    # Update last processed time if this is the newest file
                                    if file == new_files[-1]:
                                        try:
                                            file_time = datetime.strptime(file.split('.csv')[0], "%Y.%m.%d-%H.%M.%S")
                                            self.last_processed[server_id] = file_time
                                        except ValueError:
                                            # If we can't parse the timestamp from filename, use current time
                                            self.last_processed[server_id] = datetime.now()

                            except Exception as e:
                                logger.error(f"Error processing file {file}: {str(e)}")

                        # Keep the connection open for the next operation
                        return files_processed, events_processed

            except Exception as e:
                logger.error(f"SFTP error for server {server_id}: {str(e)}")
                return 0, 0
        finally:
            # This block always executes regardless of exceptions
            logger.debug(f"CSV processing completed for server {server_id}")
            # Ensure we always return a value
            return files_processed, events_processed

    async def run_historical_parse(self, server_id: str, days: int = 30) -> Tuple[int, int]:
        """Run a historical parse for a server, checking further back in time

        This function is meant to be called when setting up a new server to process
        older historical data going back further than the normal processing window.

        Args:
            server_id: Server ID to process
            days: Number of days to look back (default: 30)

        Returns:
            Tuple[int, int]: Number of files processed and events processed
        """
        # Import standardization function
        from utils.server_utils import standardize_server_id

        # Standardize server ID
        raw_server_id = server_id if server_id is not None else ""
        # Ensure we pass a string to standardize_server_id
        server_id = standardize_server_id(str(raw_server_id))
        logger.info(f"Starting historical parse for server {raw_server_id} (standardized to {server_id}), looking back {days} days")

        # Get server config
        server_configs = await self._get_server_configs()

        # Log all available server configs for debugging
        logger.info(f"Available server configs: {list(server_configs.keys())}")

        if server_id not in server_configs:
            # Try numeric comparison as fallback if server_id is numeric
            if server_id and str(server_id).isdigit():
                numeric_matches = [sid for sid in server_configs.keys() if str(sid).isdigit() and int(sid) == int(server_id)]
                if numeric_matches:
                    server_id = numeric_matches[0]
                    logger.info(f"Found server using numeric matching: {server_id}")

            # If still not found
            if server_id not in server_configs:
                logger.error(f"Server {server_id} not found in configs during historical parse")
                return 0, 0

        # Set a much earlier last_processed time to capture more history
        self.last_processed[server_id] = datetime.now() - timedelta(days=days)

        # Process CSV files with the historical window
        async with self.processing_lock:
            self.is_processing = True
            try:
                files_processed, events_processed = await self._process_server_csv_files(
                    server_id, server_configs[server_id]
                )
                logger.info(f"Historical parse complete for server {server_id}: processed {files_processed} files with {events_processed} events")
                return files_processed, events_processed
            except Exception as e:
                logger.error(f"Error in historical parse for server {server_id}: {e}")
                return 0, 0
            finally:
                self.is_processing = False

    @app_commands.command(name="process_csv")
    @app_commands.describe(
        server_id="The server ID to process CSV files for",
        hours="Number of hours to look back (default: 24)"
    )
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    @admin_permission_decorator()
    @premium_tier_required(1)  # Require Survivor tier for CSV processing
    async def process_csv_command(
        self,
        interaction: discord.Interaction,
        server_id: Optional[str] = None,
        hours: Optional[int] = 24
    ):
        """Manually process CSV files from the game server

        Args:
            interaction: Discord interaction
            server_id: Server ID to process (optional)
            hours: Number of hours to look back (default: 24)
        """

        await interaction.response.defer(ephemeral=True)

        # Import standardization function
        from utils.server_utils import standardize_server_id

        # Get server ID from guild config if not provided
        if not server_id:
            # Try to get the server ID from the guild's configuration
            try:
                guild_id = str(interaction.guild_id)
                guild_doc = await self.bot.db.guilds.find_one({"guild_id": guild_id})
                if guild_doc and "default_server_id" in guild_doc:
                    raw_server_id = guild_doc["default_server_id"]
                    server_id = standardize_server_id(raw_server_id)
                    logger.info(f"Using default server ID from guild config: {raw_server_id} (standardized to {server_id})")
                else:
                    # No default server configured
                    embed = EmbedBuilder.error(
                        title="No Server Configured",
                        description="No server ID provided and no default server configured for this guild."
                    )
                    await interaction.followup.send(embed=embed, ephemeral=True)
                    return
            except Exception as e:
                logger.error(f"Error getting default server ID: {e}")
                embed = EmbedBuilder.error(
                    title="Configuration Error",
                    description="An error occurred while retrieving the server configuration."
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return
        else:
            # Standardize the provided server ID
            raw_server_id = server_id
            server_id = standardize_server_id(server_id)
            logger.info(f"Standardized provided server ID from {raw_server_id} to {server_id}")

        # Get server config
        server_configs = await self._get_server_configs()

        # Log all available server configs for debugging
        logger.info(f"Available server configs: {list(server_configs.keys())}")

        if server_id not in server_configs:
            # Try numeric comparison as fallback if server_id is numeric
            if server_id and str(server_id).isdigit():
                numeric_matches = [sid for sid in server_configs.keys() if str(sid).isdigit() and int(sid) == int(server_id)]
                if numeric_matches:
                    server_id = numeric_matches[0]
                    logger.info(f"Found server using numeric matching: {server_id}")

            # If still not found, show error
            if server_id not in server_configs:
                embed = EmbedBuilder.error(
                    title="Server Not Found",
                    description=f"No SFTP configuration found for server `{server_id}`."
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return

        # Calculate lookback time
        # Ensure hours is a valid float value
        safe_hours = float(hours) if hours else 24.0

        # Safely update last_processed dictionary with server_id
        if server_id and isinstance(server_id, str):
            self.last_processed[server_id] = datetime.now() - timedelta(hours=safe_hours)
        else:
            logger.warning(f"Invalid server_id: {server_id}, not updating last_processed timestamp")

        # Process CSV files
        async with self.processing_lock:
            try:
                # Process files only if server_id exists in server_configs and it's a non-None string
                if server_id and isinstance(server_id, str) and server_id in server_configs:
                    files_processed, events_processed = await self._process_server_csv_files(
                        server_id, server_configs[server_id]
                    )
                else:
                    logger.error(f"Invalid server_id: {server_id} or not found in server_configs")
                    files_processed, events_processed = 0, 0

                if files_processed > 0:
                    embed = EmbedBuilder.success(
                        title="CSV Processing Complete",
                        description=f"Processed {files_processed} file(s) with {events_processed} death events."
                    )
                else:
                    embed = EmbedBuilder.info(
                        title="No Files Found",
                        description=f"No new CSV files found for server `{server_id}` in the last {hours} hours."
                    )

                await interaction.followup.send(embed=embed, ephemeral=True)

            except Exception as e:
                logger.error(f"Error processing CSV files: {str(e)}")
                embed = EmbedBuilder.error(
                    title="Processing Error",
                    description=f"An error occurred while processing CSV files: {str(e)}"
                )
                await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(name="clear_csv_cache")
    @admin_permission_decorator()
    @premium_tier_required(1)  # Require Survivor tier for CSV cache management
    async def clear_csv_cache_command(self, interaction: discord.Interaction):
        """Clear the CSV parser cache

        Args:
            interaction: Discord interaction
        """

        # Clear cache
        self.csv_parser.clear_cache()

        embed = EmbedBuilder.success(
            title="Cache Cleared",
            description="The CSV parser cache has been cleared."
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="historical_parse")
    @app_commands.describe(
        server_id="The server ID to process historical data for",
        days="Number of days to look back (default: 30)"
    )
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    @admin_permission_decorator()
    @premium_tier_required(1)  # Require Survivor tier
    async def historical_parse_command(
        self,
        interaction: discord.Interaction,
        server_id: Optional[str] = None,
        days: Optional[int] = 30
    ):
        """Process historical CSV data going back further than normal processing

        Args:
            interaction: Discord interaction
            server_id: Server ID to process (optional)
            days: Number of days to look back (default: 30)
        """
        await interaction.response.defer(ephemeral=True)

        # Import standardization function
        from utils.server_utils import standardize_server_id

        # Get server ID from guild config if not provided
        if not server_id:
            try:
                guild_id = str(interaction.guild_id)
                guild_doc = await self.bot.db.guilds.find_one({"guild_id": guild_id})
                if guild_doc and "default_server_id" in guild_doc:
                    raw_server_id = guild_doc["default_server_id"]
                    server_id = standardize_server_id(raw_server_id)
                    logger.info(f"Using default server ID from guild config: {raw_server_id} (standardized to {server_id})")
                else:
                    embed = EmbedBuilder.error(
                        title="No Server Configured",
                        description="No server ID provided and no default server configured for this guild."
                    )
                    await interaction.followup.send(embed=embed, ephemeral=True)
                    return
            except Exception as e:
                logger.error(f"Error getting default server ID: {e}")
                embed = EmbedBuilder.error(
                    title="Configuration Error",
                    description="An error occurred while retrieving the server configuration."
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return
        else:
            # Standardize the provided server ID
            raw_server_id = server_id
            server_id = standardize_server_id(server_id)
            logger.info(f"Standardized provided server ID from {raw_server_id} to {server_id}")

        # Get server config
        server_configs = await self._get_server_configs()

        # Log all available server configs for debugging
        logger.info(f"Available server configs: {list(server_configs.keys())}")

        if server_id not in server_configs:
            # Try numeric comparison as fallback if server_id is numeric
            if server_id and str(server_id).isdigit():
                numeric_matches = [sid for sid in server_configs.keys() if str(sid).isdigit() and int(sid) == int(server_id)]
                if numeric_matches:
                    server_id = numeric_matches[0]
                    logger.info(f"Found server using numeric matching: {server_id}")

            # If still not found, show error
            if server_id not in server_configs:
                embed = EmbedBuilder.error(
                    title="Server Not Found",
                    description=f"No SFTP configuration found for server `{server_id}`."
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return

        # Validate days parameter
        safe_days = max(1, min(int(days) if days else 30, 90))  # Between 1 and 90 days

        # Send initial response
        embed = EmbedBuilder.info(
            title="Historical Parsing Started",
            description=f"Starting historical parsing for server `{server_id}` looking back {safe_days} days.\n\nThis may take some time, please wait..."
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

        # Run the historical parse
        try:
            files_processed, events_processed = await self.run_historical_parse(server_id, days=safe_days)

            if files_processed > 0:
                embed = EmbedBuilder.success(
                    title="Historical Parsing Complete",
                    description=f"Processed {files_processed} historical file(s) with {events_processed} death events."
                )
            else:
                embed = EmbedBuilder.info(
                    title="No Historical Files Found",
                    description=f"No historical CSV files found for server `{server_id}` in the last {safe_days} days."
                )

            await interaction.followup.send(embed=embed, ephemeral=True)
        except Exception as e:
            logger.error(f"Error in historical parse command: {e}")
            embed = EmbedBuilder.error(
                title="Processing Error",
                description=f"An error occurred during historical parsing: {str(e)}"
            )
            await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(name="csv_status")
    @admin_permission_decorator()
    @premium_tier_required(1)  # Require Survivor tier for CSV status
    async def csv_status_command(self, interaction: discord.Interaction):
        """Show CSV processor status

        Args:
            interaction: Discord interaction
        """

        await interaction.response.defer(ephemeral=True)

        # Get server configs
        server_configs = await self._get_server_configs()

        # Create status embed
        embed = EmbedBuilder.info(
            title="CSV Processor Status",
            description="Current status of the CSV processor"
        )

        # Add processing status
        embed.add_field(
            name="Currently Processing",
            value="Yes" if self.is_processing else "No",
            inline=True
        )

        # Add configured servers
        server_list = []
        for server_id, config in server_configs.items():
            last_time = self.last_processed.get(server_id, "Never")
            if isinstance(last_time, datetime):
                last_time = last_time.strftime("%Y-%m-%d %H:%M:%S")

            server_list.append(f"• `{server_id}` - Last processed: {last_time}")

        if server_list:
            embed.add_field(
                name="Configured Servers",
                value="\n".join(server_list),
                inline=False
            )
        else:
            embed.add_field(
                name="Configured Servers",
                value="No servers configured",
                inline=False
            )

        await interaction.followup.send(embed=embed, ephemeral=True)

    async def _process_kill_event(self, event: Dict[str, Any]) -> bool:
        """Process a kill event and update player stats and rivalries

        Args:
            event: Normalized kill event dictionary

        Returns:
            bool: True if processed successfully, False otherwise
        """
        try:
            server_id = event.get("server_id")
            if not server_id:
                logger.warning("Kill event missing server_id, skipping")
                return False

            # Get kill details
            killer_id = event.get("killer_id", "")
            killer_name = event.get("killer_name", "Unknown")
            victim_id = event.get("victim_id", "")
            victim_name = event.get("victim_name", "Unknown")
            weapon = event.get("weapon", "Unknown")
            distance = event.get("distance", 0)
            timestamp = event.get("timestamp", datetime.utcnow())

            # Ensure timestamp is datetime
            if isinstance(timestamp, str):
                try:
                    timestamp = datetime.fromisoformat(timestamp)
                except ValueError:
                    timestamp = datetime.utcnow()

            # Check if this is a suicide
            is_suicide = False

            # Check based on matching IDs (both formats)
            if killer_id and victim_id and killer_id == victim_id:
                is_suicide = True

            # Check based on weapon name (post-April format specific)
            elif weapon in ["suicide_by_relocation", "suicide"]:
                is_suicide = True
                # Fix killer_id to match victim_id for consistent DB entries
                killer_id = victim_id

            # Check based on matching names if IDs don't match (data inconsistency edge case)
            elif killer_name and victim_name and killer_name == victim_name:
                logger.info(f"Detected potential suicide based on matching names: {killer_name}")
                is_suicide = True
                # Fix killer_id to match victim_id for consistent DB entries
                killer_id = victim_id

            # Check if we have the necessary player IDs
            if not victim_id:
                logger.warning("Kill event missing victim_id, skipping")
                return False

            # For suicides, we only need to update the victim's stats
            if is_suicide:
                # Get victim player or create if doesn't exist
                victim = await self._get_or_create_player(server_id, victim_id, victim_name)

                # Update suicide count
                await victim.update_stats(self.bot.db, kills=0, deaths=0, suicides=1)

                return True

            # For regular kills, we need both killer and victim
            if not killer_id:
                logger.warning("Kill event missing killer_id for non-suicide, skipping")
                return False

            # Get killer and victim players, or create if they don't exist
            killer = await self._get_or_create_player(server_id, killer_id, killer_name)
            victim = await self._get_or_create_player(server_id, victim_id, victim_name)

            # Update kill/death stats
            await killer.update_stats(self.bot.db, kills=1, deaths=0)
            await victim.update_stats(self.bot.db, kills=0, deaths=1)

            # Update rivalries
            from models.rivalry import Rivalry
            await Rivalry.record_kill(server_id, killer_id, victim_id, weapon, "")

            # Update nemesis/prey relationships
            await killer.update_nemesis_and_prey(self.bot.db)
            await victim.update_nemesis_and_prey(self.bot.db)

            # Insert kill event into database
            kill_doc = {
                "server_id": server_id,
                "killer_id": killer_id,
                "killer_name": killer_name,
                "victim_id": victim_id,
                "victim_name": victim_name,
                "weapon": weapon,
                "distance": distance,
                "timestamp": timestamp,
                "is_suicide": is_suicide
            }

            await self.bot.db.kills.insert_one(kill_doc)

            return True

        except Exception as e:
            logger.error(f"Error processing kill event: {e}")
            return False

    async def _get_or_create_player(self, server_id: str, player_id: str, player_name: str):
        """Get player by ID or create if it doesn't exist

        Args:
            server_id: Server ID
            player_id: Player ID
            player_name: Player name

        Returns:
            Player object
        """
        from models.player import Player

        # Check if player exists
        player = await Player.get_by_player_id(self.bot.db, player_id)

        if not player:
            # Create new player
            player = Player(
                player_id=player_id,
                server_id=server_id,
                name=player_name,
                display_name=player_name,
                last_seen=datetime.utcnow(),
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )

            # Insert into database
            await self.bot.db.players.insert_one(player.__dict__)

        return player

async def setup(bot: commands.Bot) -> None:
    """Set up the CSV processor cog"""
    await bot.add_cog(CSVProcessorCog(bot))