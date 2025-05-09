"""
Utilities for creating game event embeds for various event types
"""

import discord
import logging
from datetime import datetime
from typing import Dict, Any, Optional

from utils.embed_builder import EmbedBuilder
from utils.embed_icons import get_event_icon, send_embed_with_icon
from models.guild import Guild
from models.server import Server

logger = logging.getLogger(__name__)

async def create_mission_embed(mission_event: Dict[str, Any], server: Server, guild: Optional[Guild] = None) -> discord.Embed:
    """Create a Discord embed for a mission event
    
    Args:
        mission_event: Event data from the log parser
        server: Server object
        guild: Optional Guild object for themed embed
        
    Returns:
        discord.Embed: Formatted mission event embed
    """
    level = mission_event.get("mission_level", 0)
    location = mission_event.get("location", "Unknown")
    
    # Title based on mission level
    title = f"Level {level} Mission Available"
    
    # Description with location
    description = f"A high-value mission is ready at **{location}**"
    
    # Create the embed with the appropriate color based on level
    embed = EmbedBuilder.create_base_embed(title=title, description=description, guild=guild)
    
    # Color based on mission level (3=orange, 4=red)
    if level == 4:
        embed.color = discord.Color.red()
    else:  # level 3
        embed.color = discord.Color.orange()
    
    # Add server name
    embed.add_field(name="Server", value=server.server_name, inline=True)
    
    # Add event time
    timestamp = mission_event.get("timestamp", "Unknown")
    embed.add_field(name="Time", value=timestamp, inline=True)
    
    # Add mission icon
    icon_path = get_event_icon("mission")
    if icon_path is not None:
        embed.set_thumbnail(url=f"attachment://{icon_path.split('/')[-1]}")
    
    return embed

async def create_airdrop_embed(airdrop_event: Dict[str, Any], server: Server, guild: Optional[Guild] = None) -> discord.Embed:
    """Create a Discord embed for an airdrop event"""
    state = airdrop_event.get("state", "Unknown")
    
    if state == "Flying":
        title = "Air Drop Incoming"
        description = "An aircraft has been spotted flying overhead"
    else:  # "Dropping"
        title = "Air Drop Dropping"
        description = "Supplies are being dropped from the aircraft"
    
    # Create the embed with blue color
    embed = EmbedBuilder.create_base_embed(title=title, description=description, guild=guild)
    embed.color = discord.Color.blue()
    
    # Add server name
    embed.add_field(name="Server", value=server.server_name, inline=True)
    
    # Add event time
    timestamp = airdrop_event.get("timestamp", "Unknown")
    embed.add_field(name="Time", value=timestamp, inline=True)
    
    # Add appropriate icon
    icon_path = get_event_icon("airdrop")
    if icon_path is not None:
        embed.set_thumbnail(url=f"attachment://{icon_path.split('/')[-1]}")
    
    return embed

async def create_helicrash_embed(crash_event: Dict[str, Any], server: Server, guild: Optional[Guild] = None) -> discord.Embed:
    """Create a Discord embed for a helicopter crash event"""
    # Create the embed with red color
    embed = EmbedBuilder.create_base_embed(
        title="Helicopter Crash",
        description="A helicopter has crashed somewhere on the map",
        guild=guild
    )
    embed.color = discord.Color.red()
    
    # Add server name
    embed.add_field(name="Server", value=server.server_name, inline=True)
    
    # Add event time
    timestamp = crash_event.get("timestamp", "Unknown")
    embed.add_field(name="Time", value=timestamp, inline=True)
    
    # Add event ID if available is not None
    event_id = crash_event.get("event_id", None)
    if event_id is not None:
        embed.add_field(name="ID", value=event_id, inline=True)
    
    # Add appropriate icon
    icon_path = get_event_icon("crash")
    if icon_path is not None:
        embed.set_thumbnail(url=f"attachment://{icon_path.split('/')[-1]}")
    
    return embed

async def create_trader_embed(trader_event: Dict[str, Any], server: Server, guild: Optional[Guild] = None) -> discord.Embed:
    """Create a Discord embed for a trader event"""
    # Create the embed with green color
    embed = EmbedBuilder.create_base_embed(
        title="Roaming Trader",
        description="A trader has appeared on the map",
        guild=guild
    )
    embed.color = discord.Color.green()
    
    # Add server name
    embed.add_field(name="Server", value=server.server_name, inline=True)
    
    # Add event time
    timestamp = trader_event.get("timestamp", "Unknown")
    embed.add_field(name="Time", value=timestamp, inline=True)
    
    # Add event ID if available is not None
    event_id = trader_event.get("event_id", None)
    if event_id is not None:
        embed.add_field(name="ID", value=event_id, inline=True)
    
    # Add appropriate icon
    icon_path = get_event_icon("trader")
    if icon_path is not None:
        embed.set_thumbnail(url=f"attachment://{icon_path.split('/')[-1]}")
    
    return embed

async def create_convoy_embed(convoy_event: Dict[str, Any], server: Server, guild: Optional[Guild] = None) -> discord.Embed:
    """Create a Discord embed for a convoy event"""
    # Create the embed with purple color
    embed = EmbedBuilder.create_base_embed(
        title="Convoy Event",
        description="A convoy is moving through the area",
        guild=guild
    )
    embed.color = discord.Color.purple()
    
    # Add server name
    embed.add_field(name="Server", value=server.server_name, inline=True)
    
    # Add event time
    timestamp = convoy_event.get("timestamp", "Unknown")
    embed.add_field(name="Time", value=timestamp, inline=True)
    
    # Add event ID if available is not None
    event_id = convoy_event.get("event_id", None)
    if event_id is not None:
        embed.add_field(name="ID", value=event_id, inline=True)
    
    # Add appropriate icon
    icon_path = get_event_icon("convoy")
    if icon_path is not None:
        embed.set_thumbnail(url=f"attachment://{icon_path.split('/')[-1]}")
    
    return embed

async def create_event_embed(event_data: Dict[str, Any], server: Server, guild: Optional[Guild] = None) -> discord.Embed:
    """Factory function to create the appropriate event embed based on the event type
    
    Args:
        event_data: The event data from the log parser
        server: The server object
        guild: Optional Guild object for themed embed
    
    Returns:
        discord.Embed: The formatted event embed
    """
    event_type = event_data.get("event_type", "unknown")
    
    try:
        if event_type == "mission":
            return await create_mission_embed(event_data, server, guild)
        elif event_type == "airdrop":
            return await create_airdrop_embed(event_data, server, guild)
        elif event_type == "helicrash":
            return await create_helicrash_embed(event_data, server, guild)
        elif event_type == "trader":
            return await create_trader_embed(event_data, server, guild)
        elif event_type == "convoy":
            return await create_convoy_embed(event_data, server, guild)
        else:
            # Fallback for unknown event types
            embed = EmbedBuilder.create_base_embed(
                title=f"Game Event: {event_type.capitalize()}",
                description=f"A {event_type} event has occurred",
                guild=guild
            )
            
            # Add server name
            embed.add_field(name="Server", value=server.server_name, inline=True)
            
            # Add event time
            timestamp = event_data.get("timestamp", "Unknown")
            embed.add_field(name="Time", value=timestamp, inline=True)
            
            return embed
    except Exception as e:
        logger.error(f"Error creating event embed for {event_type}: {e}", exc_info=True)
        # Return a basic error embed
        return EmbedBuilder.create_error_embed(
            title="Event Processing Error",
            description=f"There was an error processing this {event_type} event",
            guild=guild
        )