"""
Utility functions for Discord command handling
"""
import logging
import discord
from typing import Any, Dict, List, Optional, Union, Tuple
from discord.ext import commands
from discord import app_commands
from utils.server_utils import standardize_server_id

logger = logging.getLogger(__name__)

# Server selection and autocomplete functions
async def get_server_selection(ctx_or_interaction, guild_id: str, db=None):
    """
    Get server selection options for a guild
    
    Args:
        ctx_or_interaction: Context or Interaction
        guild_id: Discord guild ID
        db: Optional database connection
        
    Returns:
        List of (server_id, server_name) tuples
    """
    if db is None:
        from utils.database import get_db
        db = await get_db()
    
    # Get the guild from the database
    guild_data = await db.db.guilds.find_one({"guild_id": guild_id})
    if guild_data is None:
        return []
    
    # Get the servers for this guild
    servers = guild_data.get("servers", [])
    if not servers:
        return []
    
    # Convert to list of (server_id, server_name) tuples
    server_options = []
    for server in servers:
        if isinstance(server, dict):
            server_id = server.get("server_id")
            server_name = server.get("server_name", "Unknown")
            if server_id:
                server_options.append((server_id, server_name))
        # Handle string server IDs (legacy format)
        elif isinstance(server, str):
            # Try to get the server name from the server collection
            server_data = await db.db.servers.find_one({"server_id": server})
            if server_data:
                server_name = server_data.get("server_name", "Unknown")
                server_options.append((server, server_name))
            else:
                server_options.append((server, "Unknown"))
    
    return server_options

async def server_id_autocomplete(interaction: discord.Interaction, current: str):
    """
    Autocomplete for server selection
    
    Args:
        interaction: Discord interaction
        current: Current input value
        
    Returns:
        List of app_commands.Choice options
    """
    # Get database connection
    from utils.database import get_db
    try:
        db = await get_db()
    except Exception as e:
        logger.error(f"Failed to get database connection: {e}")
        return []
    
    # Get the guild ID
    guild_id = str(interaction.guild_id) if interaction.guild_id else None
    if not guild_id:
        return []
    
    # Get server options
    server_options = await get_server_selection(interaction, guild_id, db)
    
    # Standardize all server IDs for consistency with command processing
    standardized_options = []
    for sid, name in server_options:
        # Ensure server ID is standardized the same way as in Server.get_by_id
        std_sid = standardize_server_id(str(sid) if sid is not None else "")
        if std_sid:  # Only add if standardization succeeded
            standardized_options.append((std_sid, name))
    
    # Filter by current input
    if current:
        standardized_options = [
            (sid, name) for sid, name in standardized_options
            if current.lower() in sid.lower() or current.lower() in name.lower()
        ]
    
    # Return as choices (limited to 25 as per Discord API limits)
    return [
        discord.app_commands.Choice(name=f"{name} ({sid})", value=sid)
        for sid, name in standardized_options[:25]
    ]

async def hybrid_send(interaction: Union[discord.Interaction, commands.Context], 
                     content: Optional[str] = None, 
                     embed: Optional[discord.Embed] = None,
                     ephemeral: bool = False,
                     **kwargs):
    """
    A helper function that handles sending messages in both application commands and text commands.
    
    This unified approach simplifies code by allowing the same function call pattern
    regardless of whether the command is a traditional text command or an application command.
    
    Args:
        interaction: Either a discord.Interaction (app command) or commands.Context (text command)
        content: The text content to send
        embed: A discord.Embed to send
        ephemeral: Whether the message should be ephemeral (only visible to the user who triggered the command)
        **kwargs: Additional arguments to pass to the send function
    
    Returns:
        The message that was sent
    """
    try:
        # Handle application commands (discord.Interaction)
        if isinstance(interaction, discord.Interaction):
            # Check if response is done
            if interaction.response.is_done():
                # Use followup
                return await interaction.followup.send(
                    content=content, 
                    embed=embed, 
                    ephemeral=ephemeral,
                    **kwargs
                )
            else:
                # Use response
                return await interaction.response.send_message(
                    content=content, 
                    embed=embed, 
                    ephemeral=ephemeral,
                    **kwargs
                )
        
        # Handle traditional text commands (commands.Context)
        elif isinstance(interaction, commands.Context):
            # Text commands don't support ephemeral, so we just ignore that option
            return await interaction.send(
                content=content, 
                embed=embed,
                **kwargs
            )
        
        # Handle unknown interaction type
        else:
            logger.warning(f"Unknown interaction type in hybrid_send: {type(interaction)}")
            # Just try to find a send method and call it
            if hasattr(interaction, 'send'):
                return await interaction.send(
                    content=content, 
                    embed=embed,
                    **kwargs
                )
            elif hasattr(interaction, 'followup') and hasattr(interaction.followup, 'send'):
                return await interaction.followup.send(
                    content=content, 
                    embed=embed, 
                    ephemeral=ephemeral,
                    **kwargs
                )
            else:
                logger.error(f"Cannot find a way to send a message for {type(interaction)}")
                return None
                
    except Exception as e:
        logger.error(f"Error in hybrid_send: {e}", exc_info=True)
        # Try a fallback approach if possible
        try:
            if hasattr(interaction, 'send'):
                return await interaction.send(
                    content=f"Error: {str(e)}",
                    **kwargs
                )
        except Exception as e2:
            logger.error(f"Failed to send error message: {e2}", exc_info=True)
        return None