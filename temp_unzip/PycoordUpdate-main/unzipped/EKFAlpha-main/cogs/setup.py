"""
Setup commands for configuring servers and channels
"""
import logging
import os
import re
import psutil
import discord
from discord.ext import commands
from discord import app_commands
from typing import Dict, List, Any, Optional
import asyncio
from datetime import datetime

from models.guild import Guild
from models.server import Server
from utils.sftp import SFTPClient
from utils.embed_builder import EmbedBuilder
from utils.helpers import has_admin_permission
from utils.parsers import CSVParser
from utils.decorators import premium_tier_required
from utils.discord_utils import server_id_autocomplete, hybrid_send
from config import PREMIUM_TIERS

logger = logging.getLogger(__name__)

class Setup(commands.Cog):
    """Setup commands for configuring servers and channels"""

    def __init__(self, bot):
        self.bot = bot

    @commands.hybrid_group(name="setup", description="Server setup commands")
    @commands.guild_only()
    async def setup(self, ctx):
        """Setup command group"""
        if not ctx.invoked_subcommand:
            await ctx.send("Please specify a subcommand.")

    @setup.command(name="addserver", description="Add a game server to track PvP stats")
    @app_commands.describe(
        server_name="Friendly name to display for this server",
        host="SFTP host address",
        port="SFTP port",
        username="SFTP username",
        password="SFTP password",
        server_id="Unique ID for the server (letters, numbers, underscores only)"
    )
    @app_commands.guild_only()
    @premium_tier_required(1)  # Connecting servers requires premium tier 1+
    async def add_server(self, ctx, server_name: str, host: str, port: int, username: str, password: str, server_id: str):
        """Add a new server to track"""
        try:
            # Defer response to prevent timeout
            await ctx.defer()

            # Check permissions
            if await self._check_permission(ctx):
                return

            # Get guild model for themed embed and premium checks
            # Use get_or_create which will auto-create guild if it doesn't exist
            try:
                guild_model = await Guild.get_or_create(self.bot.db, str(ctx.guild.id), ctx.guild.name)
                if not guild_model:
                    logger.error(f"Failed to get or create guild for {ctx.guild.id}")
                    await hybrid_send(ctx, "Error retrieving guild information. Please try again later.")
                    return

                logger.info(f"Retrieved guild model for {ctx.guild.id} with tier {guild_model.premium_tier}")
            except Exception as e:
                logger.error(f"Error getting guild model with get_or_create: {e}")
                await hybrid_send(ctx, "Error retrieving guild information. Please try again later.")
                return

            # Get guild premium tier and server count
            guild_tier = int(guild_model.premium_tier) if guild_model.premium_tier else 0
            server_count = len(guild_model.servers) if hasattr(guild_model, 'servers') and guild_model.servers else 0

            # Check server limit based on premium tier
            from config import PREMIUM_TIERS
            max_servers = PREMIUM_TIERS.get(guild_tier, {}).get("max_servers", 1)
            tier_name = PREMIUM_TIERS.get(guild_tier, {}).get("name", f"Tier {guild_tier}")

            if server_count >= max_servers:
                embed = await EmbedBuilder.create_error_embed(
                    "Server Limit Reached",
                    f"Your guild is on the **{tier_name}** tier, which allows a maximum of **{max_servers}** server{'s' if max_servers != 1 else ''}.\n\n"
                    f"To add more servers, please upgrade your premium tier with `/premium upgrade`.",
                    guild=guild_model
                )
                await hybrid_send(ctx, embed=embed)
                return

            # Validate server ID (no spaces, special chars except underscore)
            if not re.match(r'^[a-zA-Z0-9_]+$', server_id):
                embed = await EmbedBuilder.create_error_embed(
                    "Invalid Server ID",
                    "Server ID can only contain letters, numbers, and underscores."
                , guild=guild_model)
                await hybrid_send(ctx, embed=embed)
                return

            # Store SFTP information
            sftp_info = {
                "hostname": host,  # Changed to match SFTPClient parameter name
                "port": port,
                "username": username,
                "password": password
            }

            # Validate SFTP info
            if not host or not username or not password:
                embed = await EmbedBuilder.create_error_embed(
                    "Invalid SFTP Information",
                    "Please provide valid host, username, and password for SFTP connection."
                , guild=guild_model)
                await hybrid_send(ctx, embed=embed)
                return

            # We already have guild_model from earlier, so use that instead of fetching again
            guild = guild_model
            logger.info(f"Using existing guild model for {ctx.guild.id} with premium tier: {guild.premium_tier}")

            # Check if we can add killfeed feature
            if not guild.check_feature_access("killfeed"):
                logger.warning(f"Guild {ctx.guild.id} does not have killfeed feature access")
                embed = await EmbedBuilder.create_error_embed(
                    "Feature Disabled",
                    "This guild does not have the Killfeed feature enabled. Please contact an administrator."
                , guild=guild_model)
                await hybrid_send(ctx, embed=embed)
                return

            # Check if server ID already exists
            for server in guild.servers:
                if server.get("server_id") == server_id:
                    embed = await EmbedBuilder.create_error_embed(
                        "Server Exists",
                        f"A server with ID '{server_id}' already exists in this guild."
                    , guild=guild_model)
                    await hybrid_send(ctx, embed=embed)
                    return

            # Initial response
            embed = await EmbedBuilder.create_base_embed(
                "Adding Server",
                f"Testing connection to {server_name}..."
            , guild=guild_model)
            message = await hybrid_send(ctx, embed=embed)

            # Create SFTP client to test connection
            # For new server setup, we check if the server_id is numeric or a UUID
            # If it's numeric, use it directly for path construction
            # If it's a UUID, use the server_name to extract the numeric ID if possible
            original_server_id = server_id
            
            # If server ID looks like a UUID format, try to extract a numeric ID from server name
            if "-" in server_id and len(server_id) > 30:
                logger.info(f"Server ID appears to be in UUID format: {server_id}")
                
                # Try to find a numeric ID in the server name (often contains the numeric ID)
                for word in str(server_name).split():
                    if word.isdigit() and len(word) >= 4:
                        logger.info(f"Found potential numeric server ID in server_name: {word}")
                        original_server_id = word
                        break
            
            logger.info(f"Using original_server_id: {original_server_id} for path construction")
            
            sftp_client = SFTPClient(
                hostname=sftp_info["hostname"],  # Updated to match key in sftp_info dictionary
                port=sftp_info["port"],
                username=sftp_info["username"],
                password=sftp_info["password"],
                server_id=server_id,
                original_server_id=original_server_id  # Pass original ID for path construction
            )

            # Test connection
            connected = await sftp_client.connect()
            if not connected:
                embed = await EmbedBuilder.create_error_embed(
                    "Connection Failed",
                    f"Failed to connect to SFTP server: {sftp_client.last_error}"
                , guild=guild_model)
                await message.edit(embed=embed)
                return

            # Connection successful - skip CSV file check
            # The historical parser will find CSV files on its own
            # This eliminates redundant SFTP operations and reduces connection time
            logger.info(f"SFTP connection successful for server {server_id}. Skipping redundant CSV file check.")
            csv_files = []  # Empty placeholder since we don't need to check

            # Check if we can find log file
            embed = await EmbedBuilder.create_base_embed(
                "Adding Server",
                f"Connection successful. Looking for log file..."
            , guild=guild_model)
            await message.edit(embed=embed)

            # Get log file path but don't disconnect afterwards
            log_file = await sftp_client.get_log_file()
            log_found = bool(log_file)
            
            # Keep connection object for later use (don't disconnect)
            # This is critical to prevent the connection from being closed prematurely
            logger.info(f"Maintaining SFTP connection for server {server_id} (log_found: {log_found})")

            # Create proper Server object first
            server = await Server.create_server(
                self.bot.db,
                str(ctx.guild.id),
                server_name,
                hostname=sftp_info["hostname"],
                port=sftp_info["port"],
                username=sftp_info["username"],
                password=sftp_info["password"],
                sftp_host=sftp_info["hostname"],
                sftp_port=sftp_info["port"],
                sftp_username=sftp_info["username"],
                sftp_password=sftp_info["password"],
                original_server_id=original_server_id  # Pass the extracted or derived original server ID
            )

            if not server:
                embed = await EmbedBuilder.create_error_embed(
                    "Server Creation Failed",
                    "Failed to create server entry in database."
                , guild=guild_model)
                await message.edit(embed=embed)
                return

            # Build separate paths for logs and CSV
            base_server_dir = f"{sftp_info['hostname'].split(':')[0]}_{original_server_id}"
            
            # Log parser path (in /Logs)
            log_parser_path = os.path.join("/", base_server_dir, "Logs")
            
            # CSV parser path (in /actual1/deathlogs)
            csv_parser_base = os.path.join("/", base_server_dir, "actual1", "deathlogs")
            
            # Create server data with separated parser paths
            server_data = {
                "server_id": server.server_id,
                "original_server_id": original_server_id,
                "server_name": server_name,
                "guild_id": str(ctx.guild.id),
                "sftp_host": sftp_info["hostname"],
                "sftp_port": sftp_info["port"],
                "sftp_username": sftp_info["username"],
                "sftp_password": sftp_info["password"],
                "hostname": sftp_info["hostname"],
                "port": sftp_info["port"],
                "username": sftp_info["username"],
                "password": sftp_info["password"],
                "last_csv_line": 0,
                "last_log_line": 0,
                "sftp_enabled": True,
                # Separate paths for each parser
                "log_parser_path": log_parser_path,  # For log parser only
                "csv_parser_path": csv_parser_base   # For CSV parser only
            }
            
            # Log both IDs for diagnostic purposes
            logger.info(f"Adding server with UUID: {server.server_id} and original ID: {original_server_id}")

            # Add server to guild
            add_result = await guild.add_server(server_data)
            if not add_result:
                embed = await EmbedBuilder.create_error_embed(
                    "Error Adding Server",
                    "Failed to add server to the database. This may be due to a server limit restriction."
                , guild=guild_model)
                await message.edit(embed=embed)
                # Keep the connection in the pool, don't disconnect
                # await sftp_client.disconnect()
                return

            # Success message
            embed = await EmbedBuilder.create_success_embed(
                "Server Added Successfully",
                f"Server '{server_name}' has been added and is ready for channel setup."
            , guild=guild_model)

            # Add connection details
            connection_status = [
                f"SFTP Connection: Successful",
                f"Log File: {'Found' if log_found else 'Not found'}",
                f"CSV Files: Will be located during historical parsing",
                f"",
                f"**Note**: Historical parsing will begin in 30 seconds to ensure database synchronization."
            ]
            embed.add_field(
                name="Connection Status", 
                value="\n".join(connection_status),
                inline=False
            )

            # Add next steps
            next_steps = [
                "Use `/setup channels <server>` to configure notification channels.",
                "Use `/killfeed start <server>` to start monitoring the killfeed.",
                "If you have premium, use `/events start <server>` to monitor game events."
            ]
            embed.add_field(
                name="Next Steps", 
                value="\n".join(next_steps),
                inline=False
            )

            await message.edit(embed=embed)
            # Don't disconnect the SFTP client here
            # The connection will be reused by the historical parser and other processes
            # await sftp_client.disconnect()

            # Start historical parsing automatically
            try:
                # Update the message with parsing info
                embed = await EmbedBuilder.create_info_embed(
                    "Historical Parse Starting",
                    f"Starting automatic historical data parsing for server '{server_name}'."
                    + "\n\nThis process will run in the background and may take some time depending on the amount of data."
                , guild=guild_model)
                await message.edit(embed=embed)
                
                # Actually start the historical parse - find CSV processor cog
                csv_processor_cog = None
                for cog_name, cog in self.bot.cogs.items():
                    if cog_name == "CSVProcessorCog" or hasattr(cog, "run_historical_parse"):
                        csv_processor_cog = cog
                        break
                
                if csv_processor_cog and hasattr(csv_processor_cog, "run_historical_parse"):
                    # Create a background task for historical parsing
                    task_name = f"historical_parse_{ctx.guild.id}_{server_id}"
                    
                    async def run_historical_parse_task():
                        try:
                            # Add a delay to ensure database writes are complete before starting the parse
                            delay_seconds = 30
                            logger.info(f"Waiting {delay_seconds} seconds before starting historical parse for server {server_id} to ensure database writes are complete")
                            await asyncio.sleep(delay_seconds)
                            
                            logger.info(f"Starting historical parse task for server {server_id}")
                            # Use a longer lookback period (30 days) for initial setup
                            files_processed, events_processed = await csv_processor_cog.run_historical_parse(server_id, days=30)
                            
                            # Log the results
                            logger.info(f"Historical parse complete for server {server_id}: processed {files_processed} files with {events_processed} events")
                            
                            # Update message if possible
                            try:
                                updated_embed = await EmbedBuilder.create_success_embed(
                                    "Historical Parse Complete",
                                    f"Completed historical data parsing for server '{server_name}'.\n\n"
                                    f"Processed {files_processed} CSV files and {events_processed} death events."
                                , guild=guild_model)
                                await message.edit(embed=updated_embed)
                            except Exception as msg_err:
                                logger.error(f"Error updating message after historical parse: {msg_err}")
                        except Exception as task_err:
                            logger.error(f"Error in historical parse task: {task_err}")
                    
                    # Create and start the task
                    task = asyncio.create_task(run_historical_parse_task())
                    self.bot.background_tasks[task_name] = task
                    logger.info(f"Started historical parse task: {task_name}")
                else:
                    logger.error("Could not find CSV processor cog for historical parsing")
            except Exception as e:
                logger.error(f"Error starting historical parse for new server: {e}")

        except Exception as e:
            logger.error(f"Error in add_server command: {e}", exc_info=True)
            try:
                await hybrid_send(ctx, f"An error occurred while adding the server: {str(e)}")
            except:
                pass

    @setup.command(name="removeserver", description="Remove a server from tracking")
    @app_commands.describe(
        server_id="ID of the server to remove"
    )
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    @app_commands.guild_only()
    async def remove_server(self, ctx, server_id: str):
        """Remove a server from tracking"""
        try:
            # Defer response to prevent timeout
            await ctx.defer()

            # Check permissions
            if await self._check_permission(ctx):
                return

            # Log the raw server ID from the command for diagnostic purposes
            logger.info(f"Remove server command called with server_id: '{server_id}', type: {type(server_id)}")
            
            # Import standardization function
            from utils.server_utils import standardize_server_id
            std_server_id = standardize_server_id(server_id)
            logger.info(f"Standardized server_id for removal: '{std_server_id}'")

            # Get guild model with proper error handling
            try:
                guild_model = await Guild.get_or_create(self.bot.db, str(ctx.guild.id), ctx.guild.name)
                if not guild_model:
                    raise ValueError("Failed to get or create guild model")
                    
                # Log all servers in guild for debugging
                logger.info(f"Servers in guild {ctx.guild.id} ({ctx.guild.name}):")
                for i, s in enumerate(guild_model.servers):
                    s_id = s.get("server_id")
                    s_name = s.get("server_name", "Unknown")
                    std_id = standardize_server_id(s_id)
                    logger.info(f"  - Server {i}: ID={s_id}, StdID={std_id}, Name={s_name}, Type={type(s_id)}")
                    
            except Exception as e:
                logger.error(f"Error getting guild model: {e}")
                embed = await EmbedBuilder.create_error_embed(
                    "Database Error",
                    "Failed to access guild configuration. Please try again later."
                )
                await hybrid_send(ctx, embed=embed)
                return

            # First check if server exists in guild.servers directly
            # This addresses the issue where Server.get_by_id might not find a server
            # but it's still visible in the autocomplete and guild configuration
            server_in_guild = None
            for s in guild_model.servers:
                s_id = s.get("server_id")
                std_id = standardize_server_id(s_id)
                
                # Try multiple matching approaches
                if (s_id == server_id or  # Direct match
                    std_id == std_server_id or  # Standardized match
                    (std_id.isdigit() and std_server_id.isdigit() and int(std_id) == int(std_server_id))):  # Numeric match
                    server_in_guild = s
                    logger.info(f"Found server in guild.servers: {s_id} (matched using {'direct' if s_id == server_id else 'standardized' if std_id == std_server_id else 'numeric'} comparison)")
                    break
            
            # Get server from database using proper lookup
            server = await Server.get_by_id(self.bot.db, str(server_id), str(ctx.guild.id))
            
            # If server not found in game_servers but exists in guild.servers, create a temporary server object
            if not server and server_in_guild:
                logger.info(f"Server not found in game_servers but exists in guild.servers, creating temporary server object")
                # Create a basic server object from the guild.servers entry
                from models.server import Server as ServerModel
                server = ServerModel(
                    db=self.bot.db,
                    server_id=std_server_id,
                    guild_id=str(ctx.guild.id),
                    name=server_in_guild.get("server_name", "Unknown Server"),
                    sftp_host=server_in_guild.get("sftp_host", ""),
                    sftp_port=server_in_guild.get("sftp_port", 22),
                    sftp_username=server_in_guild.get("sftp_username", ""),
                    sftp_password=server_in_guild.get("sftp_password", ""),
                    log_path=server_in_guild.get("log_path", "")
                )
                
            if not server:
                embed = await EmbedBuilder.create_error_embed(
                    "Server Not Found", 
                    f"Could not find server with ID '{server_id}'.",
                    guild=guild_model
                )
                await hybrid_send(ctx, embed=embed)
                return

            # Add confirmation button
            class ConfirmView(discord.ui.View):
                def __init__(self, bot, timeout=60):
                    self.bot = bot
                    super().__init__(timeout=timeout)

                @discord.ui.button(label="Confirm", style=discord.ButtonStyle.danger)
                async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
                    if interaction.user.id != ctx.author.id:
                        await interaction.response.send_message("You cannot use this button.", ephemeral=True)
                        return

                    try:
                        # Capture all the variables we need before deletion
                        server_name = server.name
                        server_id_val = server.server_id
                        guild_id_val = str(ctx.guild.id)
                        
                        # Import server utils here for consistent ID handling
                        from utils.server_utils import standardize_server_id
                        std_server_id = standardize_server_id(server_id_val)
                        
                        # First update the guild model directly (this is safer than deletion)
                        logger.info(f"Removing server from guild first. Raw ID: {server_id_val}, Type: {type(server_id_val)}")
                        logger.info(f"Standardized ID for removal: {std_server_id}")
                        
                        # Make sure we have the latest version of the guild model
                        fresh_guild_model = await Guild.get_by_id(self.bot.db, guild_id_val)
                        if fresh_guild_model:
                            await fresh_guild_model.remove_server(std_server_id)
                            logger.info(f"Successfully removed server from guild.servers array")
                        else:
                            logger.warning(f"Could not get fresh guild model for {guild_id_val}")
                            
                        # Now use the server object to delete from all collections
                        success = await server.delete(self.bot.db)
                        if not success:
                            logger.warning(f"Server.delete() reported no deletions, but continuing with task cleanup")
                        
                        # Also try direct deletion from each collection as a backup
                        try:
                            # Delete using standardized ID from game_servers
                            result = await self.bot.db.game_servers.delete_many({"server_id": std_server_id})
                            logger.info(f"Backup deletion from game_servers: {result.deleted_count} documents")
                            
                            # Delete from servers collection too
                            result = await self.bot.db.servers.delete_many({"server_id": std_server_id})
                            logger.info(f"Backup deletion from servers: {result.deleted_count} documents")
                        except Exception as deletion_err:
                            logger.error(f"Error in backup deletion: {deletion_err}")

                        # Stop running tasks - use raw server_id from command for consistent lookup
                        task_names = [
                            f"killfeed_{guild_id_val}_{server_id}",  # Original server_id from command
                            f"events_{guild_id_val}_{server_id}"
                        ]

                        for task_name in task_names:
                            try:
                                if task_name in self.bot.background_tasks:
                                    task = self.bot.background_tasks[task_name]
                                    if not task.done() and not task.cancelled():
                                        task.cancel()
                                    del self.bot.background_tasks[task_name]
                                    logger.info(f"Stopped task: {task_name}")
                            except Exception as task_err:
                                logger.error(f"Error stopping task {task_name}: {task_err}")

                        # Create success message
                        embed = await EmbedBuilder.create_success_embed(
                            "Server Removed",
                            f"Successfully removed server '{server_name}'.",
                            guild=fresh_guild_model or guild_model  # Use fresh model if available
                        )
                        
                        # Send response - handle interaction errors
                        try:
                            await interaction.response.edit_message(embed=embed, view=None)
                        except discord.errors.InteractionResponded:
                            # If already responded, use followup
                            await interaction.followup.send(embed=embed)
                        except Exception as resp_err:
                            logger.error(f"Error responding to interaction: {resp_err}")
                            # Try one more fallback method
                            try:
                                if hasattr(interaction, 'edit_original_response'):
                                    await interaction.edit_original_response(embed=embed, view=None)
                            except:
                                pass

                    except Exception as e:
                        logger.error(f"Error removing server: {e}")
                        embed = await EmbedBuilder.create_error_embed(
                            "Error",
                            f"Failed to remove server: {str(e)}",
                            guild=guild_model
                        )
                        
                        # Handle interaction errors properly
                        try:
                            await interaction.response.edit_message(embed=embed, view=None)
                        except discord.errors.InteractionResponded:
                            # If already responded, use followup
                            await interaction.followup.send(embed=embed)
                        except Exception as resp_err:
                            logger.error(f"Error responding to interaction in error handler: {resp_err}")
                            # Try one more fallback method
                            try:
                                if hasattr(interaction, 'edit_original_response'):
                                    await interaction.edit_original_response(embed=embed, view=None)
                            except:
                                pass

                @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
                async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
                    if interaction.user.id != ctx.author.id:
                        await interaction.response.send_message("You cannot use this button.", ephemeral=True)
                        return

                    embed = await EmbedBuilder.create_info_embed(
                        "Cancelled",
                        "Server removal cancelled.",
                        guild=guild_model
                    )
                    
                    # Handle interaction errors properly
                    try:
                        await interaction.response.edit_message(embed=embed, view=None)
                    except discord.errors.InteractionResponded:
                        # If already responded, use followup
                        await interaction.followup.send(embed=embed)
                    except Exception as resp_err:
                        logger.error(f"Error responding to interaction in cancel button: {resp_err}")
                        # Try one more fallback method
                        try:
                            if hasattr(interaction, 'edit_original_response'):
                                await interaction.edit_original_response(embed=embed, view=None)
                        except:
                            pass

            # Create confirmation embed
            embed = await EmbedBuilder.create_warning_embed(
                "Confirm Server Removal",
                f"Are you sure you want to remove server '{server.name}' (ID: {server.server_id})?\n\n"
                "This will remove all server configurations, stop monitoring, and delete historical data. "
                "This action cannot be undone.",
                guild=guild_model
            )

            # Send confirmation message with the bot instance passed to the view
            await hybrid_send(ctx, embed=embed, view=ConfirmView(self.bot))

        except Exception as e:
            logger.error(f"Error in remove_server command: {e}")
            try:
                await hybrid_send(ctx, f"An error occurred: {str(e)}")
            except:
                pass

    @setup.command(name="list_servers", description="List all configured servers for this guild")
    @app_commands.guild_only()
    async def list_servers_command(self, ctx):
        """List all configured servers for this guild"""
        try:
            # For slash commands, defer response to prevent timeout
            await ctx.defer()

            # Get guild model
            guild_id = str(ctx.guild.id)
            guild = await Guild.get_by_id(self.bot.db, guild_id)
            if not guild:
                await hybrid_send(ctx, "No configuration found for this guild.")
                return

            # Get server count
            server_count = len(guild.servers) if hasattr(guild, 'servers') and guild.servers else 0

            if server_count == 0:
                embed = await EmbedBuilder.create_info_embed(
                    "No Servers Configured",
                    "This guild has no game servers configured yet. Use `/setup addserver` to add one.",
                    guild=guild
                )
                await hybrid_send(ctx, embed=embed)
                return

            # Create embed with server list
            embed = await EmbedBuilder.create_info_embed(
                "Configured Servers",
                f"This guild has {server_count} server(s) configured:",
                guild=guild
            )

            # Get premium tier
            premium_tier = int(guild.premium_tier) if guild.premium_tier else 0
            tier_name = PREMIUM_TIERS.get(premium_tier, {}).get("name", f"Tier {premium_tier}")
            max_servers = PREMIUM_TIERS.get(premium_tier, {}).get("max_servers", 1)

            # Add premium tier info
            embed.add_field(
                name="Premium Tier",
                value=f"{tier_name} ({server_count}/{max_servers} servers used)",
                inline=False
            )

            # Add each server
            for server in guild.servers:
                server_name = server.get("server_name", "Unknown")
                server_id = server.get("server_id", "unknown")
                
                killfeed_status = "Not started"
                if f"killfeed_{guild_id}_{server_id}" in self.bot.background_tasks:
                    task = self.bot.background_tasks[f"killfeed_{guild_id}_{server_id}"]
                    if task.done():
                        killfeed_status = "Stopped"
                    else:
                        killfeed_status = "Running"
                
                sftp_host = server.get("sftp_host", "Not set")
                if sftp_host and server.get("sftp_port"):
                    sftp_host = f"{sftp_host}:{server.get('sftp_port')}"
                
                # Create server field
                field_value = [
                    f"**ID:** {server_id}",
                    f"**Status:** {killfeed_status}",
                    f"**SFTP:** {sftp_host}"
                ]
                
                embed.add_field(
                    name=server_name,
                    value="\n".join(field_value),
                    inline=True
                )

            await hybrid_send(ctx, embed=embed)

        except Exception as e:
            logger.error(f"Error listing servers: {e}")
            await hybrid_send(ctx, f"An error occurred: {str(e)}")

    async def _check_permission(self, ctx):
        """Check if user has admin permissions. Returns True if user doesn't have permissions."""
        if not has_admin_permission(ctx):
            embed = await EmbedBuilder.create_error_embed(
                "Permission Denied",
                "You need administrator permissions to use this command."
            )
            await hybrid_send(ctx, embed=embed)
            return True
        return False

    @setup.command(name="channels", description="Configure notification channels")
    @app_commands.guild_only()
    @app_commands.describe(
        server_id="ID of the server to configure",
        killfeed_channel="Channel for kill notifications"
    )
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    async def configure_channels(self, ctx, server_id: str, killfeed_channel: discord.TextChannel = None):
        """Configure notification channels for a server"""
        try:
            # Defer response to prevent timeout
            await ctx.defer()

            # Check permissions
            if await self._check_permission(ctx):
                return

            # Get guild and server models
            guild_model = await Guild.get_or_create(self.bot.db, str(ctx.guild.id), ctx.guild.name)
            if not guild_model:
                await hybrid_send(ctx, "Error retrieving guild configuration.")
                return

            # Get server from guild configuration
            server_data = None
            for server in guild_model.servers:
                if server.get("server_id") == server_id:
                    server_data = server
                    break

            if not server_data:
                embed = await EmbedBuilder.create_error_embed(
                    "Server Not Found",
                    f"Server with ID '{server_id}' not found in this guild's configuration.",
                    guild=guild_model
                )
                await hybrid_send(ctx, embed=embed)
                return

            # Update channels configuration
            updated = False

            if killfeed_channel:
                channel_id = str(killfeed_channel.id)
                if "channels" not in server_data:
                    server_data["channels"] = {}
                server_data["channels"]["killfeed"] = channel_id
                updated = True

            # Update server configuration in guild model
            if updated:
                for i, server in enumerate(guild_model.servers):
                    if server.get("server_id") == server_id:
                        guild_model.servers[i] = server_data
                        break

                # Save updated guild model
                result = await guild_model.save(self.bot.db)
                if not result:
                    embed = await EmbedBuilder.create_error_embed(
                        "Update Failed",
                        "Failed to update channel configuration.",
                        guild=guild_model
                    )
                    await hybrid_send(ctx, embed=embed)
                    return

                # Success message
                embed = await EmbedBuilder.create_success_embed(
                    "Channels Configured",
                    f"Channel configuration updated for server '{server_data.get('server_name')}'.",
                    guild=guild_model
                )

                # Add channel overview
                channels = []
                if "channels" in server_data:
                    for channel_type, channel_id in server_data["channels"].items():
                        channel_name = f"<#{channel_id}>"
                        channels.append(f"**{channel_type.capitalize()}:** {channel_name}")

                if channels:
                    embed.add_field(
                        name="Configured Channels",
                        value="\n".join(channels),
                        inline=False
                    )

                await hybrid_send(ctx, embed=embed)
            else:
                # No changes made
                embed = await EmbedBuilder.create_info_embed(
                    "No Changes",
                    "No channel configuration changes were made.",
                    guild=guild_model
                )
                await hybrid_send(ctx, embed=embed)

        except Exception as e:
            logger.error(f"Error configuring channels: {e}")
            await hybrid_send(ctx, f"An error occurred: {str(e)}")

async def setup(bot):
    """Setup function for the Setup cog"""
    await bot.add_cog(Setup(bot))