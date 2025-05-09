"""
Command decorators for the Tower of Temptation PvP Statistics Discord Bot.

This module provides decorators for Discord.py commands to enforce:
1. Guild-based permission checks
2. Premium feature access validation
3. Server validation with cross-guild isolation
4. Error handling and rate limiting
5. Command metrics and performance tracking

These decorators can be applied to both traditional commands and slash commands
with consistent behavior and validation patterns.
"""
import logging
import asyncio
import time
import functools
import traceback
from datetime import datetime, timedelta
from typing import (
    Callable, Optional, List, Dict, Any, Union, TypeVar, 
    Coroutine, cast, Awaitable, Set, Tuple
)

import discord
from discord.ext import commands
from discord import app_commands

from config import PREMIUM_TIERS, COMMAND_PREFIX as PREFIX
from utils.premium import (
    validate_premium_feature, validate_server_limit, 
    get_guild_premium_tier, check_tier_access
)
from utils.server_utils import (
    standardize_server_id, validate_server_id_format,
    get_server_safely, check_server_existence, enforce_guild_isolation,
    validate_server, check_server_exists
)
from utils.helpers import is_home_guild_admin
from models.guild import Guild
from utils.async_utils import AsyncCache, retryable

logger = logging.getLogger(__name__)

# Type variables for generics
T = TypeVar('T')
CommandT = TypeVar('CommandT', bound=Callable[..., Coroutine[Any, Any, Any]])
SlashCommandT = TypeVar('SlashCommandT', bound=Callable[..., Coroutine[Any, Any, Any]])

# Cache configuration
COMMAND_GUILD_CACHE_TTL = 60  # 1 minute
COMMAND_COOLDOWNS = {}  # Map of user IDs to command timestamp
ERROR_TRACKING = {}  # Map of command names to error counts
COMMAND_METRICS = {}  # Map of command names to metrics (invoke count, avg runtime)

# AsyncCache instance for guild objects in commands
guild_cache = AsyncCache(ttl=COMMAND_GUILD_CACHE_TTL)

# Track commands with high error rates
HIGH_ERROR_THRESHOLD = 0.25  # 25% error rate is considered high
ERROR_COUNT_THRESHOLD = 5    # Minimum error count for tracking
MAX_SLOW_COMMANDS = 10       # Number of slow commands to track
SLOW_COMMAND_THRESHOLD = 1.0  # Command is considered slow if it takes more than 1 second


def premium_tier_required(tier_level: int):
    """
    Decorator to check if the guild has the required premium tier level.
    
    This decorator implements tier inheritance, ensuring higher tiers 
    have access to all features from lower tiers.
    
    This works on both traditional commands and application commands.
    
    Args:
        tier_level: Minimum tier level required (0-4)
        
    Returns:
        Command decorator
    """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Determine if this is a traditional command or application command
            if args and hasattr(args[0], 'bot'):
                # Traditional command
                cog = args[0] 
                # For traditional commands, context is the second arg
                if len(args) > 1 and isinstance(args[1], commands.Context):
                    ctx = args[1]
                    
                    # Skip check in DMs (no guild)
                    if ctx and not ctx.guild:
                        return await func(*args, **kwargs)
                        
                    # Get guild model
                    db = cog.bot.db if hasattr(cog.bot, 'db') else None
                    if not db:
                        logger.error("Cannot check premium tier: bot.db is not available")
                        return await func(*args, **kwargs)
                        
                    # Log what we're checking
                    logger.debug(f"Checking premium tier access for command: {func.__name__}, required tier: {tier_level}")
                    
                    # Check premium tier access - this function handles tier inheritance
                    has_access, error_message = await check_tier_access(db, ctx.guild.id, tier_level)
                    
                    # If has_access is False, send the error message
                    if has_access is False:
                        if error_message:
                            await ctx.send(error_message)
                        return None
                        
                    # Access is granted, continue with command
                    return await func(*args, **kwargs)
                    
            elif args and hasattr(args[0], 'client'):
                # Application command
                cog = args[0]
                # For app commands, interaction is the second arg
                if len(args) > 1 and isinstance(args[1], discord.Interaction):
                    interaction = args[1]
                    
                    # Skip check in DMs (no guild)
                    if interaction and not interaction.guild:
                        return await func(*args, **kwargs)
                        
                    # Get guild model
                    db = cog.client.db if hasattr(cog.client, 'db') else None
                    if not db:
                        logger.error("Cannot check premium tier: client.db is not available")
                        return await func(*args, **kwargs)
                    
                    # Log what we're checking
                    logger.debug(f"Checking premium tier access for slash command: {func.__name__}, required tier: {tier_level}")
                    
                    # Check premium tier access - this function handles tier inheritance
                    has_access, error_message = await check_tier_access(db, interaction.guild_id, tier_level)
                    
                    # If has_access is False, send the error message
                    if has_access is False:
                        if error_message:
                            try:
                                await interaction.response.send_message(error_message, ephemeral=True)
                            except Exception as e:
                                logger.error(f"Error sending tier access error: {e}")
                                # Fallback to deferred responses if response already sent
                                try:
                                    await interaction.followup.send(error_message, ephemeral=True)
                                except Exception as e2:
                                    logger.error(f"Failed to send followup message: {e2}")
                        return None
                    
                    # Access is granted, continue with command
                    return await func(*args, **kwargs)
                    
            # If we can't determine the command type or context, just run the command
            return await func(*args, **kwargs)
            
        return wrapper
    return decorator


def requires_premium_feature(feature_name: str):
    """
    Decorator to check if the guild has access to a premium feature.
    
    This decorator implements tier inheritance, ensuring higher tiers
    have access to all features from lower tiers.
    
    This works on both traditional commands and application commands.
    
    Args:
        feature_name: Name of the feature to check
        
    Returns:
        Command decorator
    """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Determine if this is a traditional command or application command
            if args and hasattr(args[0], 'bot'):
                # Traditional command
                cog = args[0] 
                # For traditional commands, context is the second arg
                if len(args) > 1 and isinstance(args[1], commands.Context):
                    ctx = args[1]
                    
                    # Skip check in DMs (no guild)
                    if ctx and not ctx.guild:
                        return await func(*args, **kwargs)
                        
                    # Get guild model
                    db = cog.bot.db if hasattr(cog.bot, 'db') else None
                    if not db:
                        logger.error("Cannot check premium feature: bot.db is not available")
                        return await func(*args, **kwargs)
                        
                    guild_model = await Guild.get_by_id(db, ctx.guild.id)
                    
                    # Log what we're checking
                    logger.debug(f"Checking premium feature access for command: {func.__name__}, feature: {feature_name}")
                    
                    # Check premium feature access - this function handles tier inheritance
                    has_access, error_message = await validate_premium_feature(guild_model, feature_name)
                    
                    # If has_access is False, send the error message
                    if has_access is False:
                        if error_message:
                            await ctx.send(error_message)
                        return None
                        
                    # Access is granted, continue with command
                    return await func(*args, **kwargs)
                    
            elif args and hasattr(args[0], 'client'):
                # Application command
                cog = args[0]
                # For app commands, interaction is the second arg
                if len(args) > 1 and isinstance(args[1], discord.Interaction):
                    interaction = args[1]
                    
                    # Skip check in DMs (no guild)
                    if interaction and not interaction.guild:
                        return await func(*args, **kwargs)
                        
                    # Get guild model
                    db = cog.client.db if hasattr(cog.client, 'db') else None
                    if not db:
                        logger.error("Cannot check premium feature: client.db is not available")
                        return await func(*args, **kwargs)
                        
                    guild_model = await Guild.get_by_id(db, interaction.guild_id)
                    
                    # Log what we're checking
                    logger.debug(f"Checking premium feature access for slash command: {func.__name__}, feature: {feature_name}")
                    
                    # Check premium feature access - this function handles tier inheritance
                    has_access, error_message = await validate_premium_feature(guild_model, feature_name)
                    
                    # If has_access is False, send the error message
                    if has_access is False:
                        if error_message:
                            try:
                                await interaction.response.send_message(error_message, ephemeral=True)
                            except Exception as e:
                                logger.error(f"Error sending feature access error: {e}")
                                # Fallback to deferred responses if response already sent
                                try:
                                    await interaction.followup.send(error_message, ephemeral=True)
                                except Exception as e2:
                                    logger.error(f"Failed to send followup message: {e2}")
                        return None
                        
                    # Access is granted, continue with command
                    return await func(*args, **kwargs)
                    
            # If we can't determine the command type or context, just run the command
            return await func(*args, **kwargs)
            
        return wrapper
    return decorator


def validate_guild_server(server_id_param: str = "server_id"):
    """
    Decorator to validate server ID belongs to the guild and exists.
    
    For both traditional commands and application commands.
    
    Args:
        server_id_param: Name of the parameter containing the server ID
        
    Returns:
        Command decorator
    """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Get server_id from kwargs
            server_id = kwargs.get(server_id_param)
            if not server_id or server_id == "":
                # If server_id not in kwargs, check command args
                for i, arg in enumerate(args):
                    # Skip self/cog
                    if i == 0:
                        continue
                    # Skip context/interaction
                    if i == 1 and isinstance(arg, (commands.Context, discord.Interaction)):
                        continue
                    # Assume next positional arg might be server_id
                    if isinstance(arg, (str, int)):
                        server_id = str(arg)
                        break
            
            if not server_id or server_id == "":
                logger.warning(f"Cannot validate server: {server_id_param} not found in args or kwargs")
                return await func(*args, **kwargs)
                
            # Determine if this is a traditional command or application command
            cog = None
            guild_id = None
            error_callback = None
            db = None
                
            if args and hasattr(args[0], 'bot'):
                # Traditional command
                cog = args[0]
                if len(args) > 1 and isinstance(args[1], commands.Context):
                    ctx = args[1]
                    # Skip check in DMs (no guild)
                    if ctx and not ctx.guild:
                        return await func(*args, **kwargs)
                    guild_id = str(ctx.guild.id)
                    db = cog.bot.db if hasattr(cog.bot, 'db') else None
                    # Error callback defined as a separate function to allow await
                    async def send_error(msg):
                        await ctx.send(msg)
                    error_callback = send_error
                    
            elif args and hasattr(args[0], 'client'):
                # Application command
                cog = args[0]
                if len(args) > 1 and isinstance(args[1], discord.Interaction):
                    interaction = args[1]
                    # Skip check in DMs (no guild)
                    if interaction and not interaction.guild:
                        return await func(*args, **kwargs)
                    guild_id = str(interaction.guild_id)
                    db = cog.client.db if hasattr(cog.client, 'db') else None
                    # Error callback defined as a separate function to allow await
                    async def send_error(msg):
                        await interaction.response.send_message(msg, ephemeral=True)
                    error_callback = send_error
            
            # If we can't determine the command type or context, just run the command
            if not guild_id or guild_id == "" or not db:
                return await func(*args, **kwargs)
                
            # Validate server
            guild_model = await Guild.get_by_id(db, guild_id)
            is_valid, error_message = await validate_server(guild_model, server_id)
            
            if not is_valid:
                if error_message and error_callback:
                    await error_callback(error_message)
                return
                
            return await func(*args, **kwargs)
            
        return wrapper
    return decorator


def requires_home_guild_admin():
    """
    Decorator to check if the user is a home guild admin.
    
    This works on both traditional commands and application commands.
    
    Returns:
        Command decorator
    """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Determine if this is a traditional command or application command
            if args and hasattr(args[0], 'bot'):
                # Traditional command
                cog = args[0]
                # For traditional commands, context is the second arg
                if len(args) > 1 and isinstance(args[1], commands.Context):
                    ctx = args[1]
                    
                    # Skip check in DMs (no guild)
                    if ctx and not ctx.guild:
                        await ctx.send("This command can only be used in a server.")
                        return
                    
                    # Check if user is a home guild admin
                    if not is_home_guild_admin(cog.bot, ctx.author.id):
                        await ctx.send("Only home guild administrators can use this command.")
                        return
                    
                    return await func(*args, **kwargs)
                    
            elif args and hasattr(args[0], 'client'):
                # Application command
                cog = args[0]
                # For app commands, interaction is the second arg
                if len(args) > 1 and isinstance(args[1], discord.Interaction):
                    interaction = args[1]
                    
                    # Skip check in DMs (no guild)
                    if interaction and not interaction.guild:
                        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
                        return
                    
                    # Check if user is a home guild admin
                    if not is_home_guild_admin(cog.client, interaction.user.id):
                        await interaction.response.send_message("Only home guild administrators can use this command.", ephemeral=True)
                        return
                    
                    return await func(*args, **kwargs)
            
            # If we can't determine the command type or context, just run the command
            return await func(*args, **kwargs)
            
        return wrapper
    return decorator


def has_admin_permission():
    """
    Decorator to check if the user has admin permissions.
    
    This works on both traditional commands and application commands.
    
    Returns:
        Command decorator
    """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Determine if this is a traditional command or application command
            if args and hasattr(args[0], 'bot'):
                # Traditional command
                cog = args[0]
                # For traditional commands, context is the second arg
                if len(args) > 1 and isinstance(args[1], commands.Context):
                    ctx = args[1]
                    
                    # Skip check in DMs (no guild)
                    if ctx and not ctx.guild:
                        await ctx.send("This command can only be used in a server.")
                        return
                    
                    # Check if user is admin
                    if not ctx.author.guild_permissions.administrator:
                        await ctx.send("You need administrator permissions to use this command.")
                        return
                    
                    return await func(*args, **kwargs)
                    
            elif args and hasattr(args[0], 'client'):
                # Application command
                cog = args[0]
                # For app commands, interaction is the second arg
                if len(args) > 1 and isinstance(args[1], discord.Interaction):
                    interaction = args[1]
                    
                    # Skip check in DMs (no guild)
                    if interaction and not interaction.guild:
                        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
                        return
                    
                    # Check if user is admin
                    member = interaction.guild.get_member(interaction.user.id)
                    if not member or not member.guild_permissions.administrator:
                        await interaction.response.send_message("You need administrator permissions to use this command.", ephemeral=True)
                        return
                    
                    return await func(*args, **kwargs)
            
            # If we can't determine the command type or context, just run the command
            return await func(*args, **kwargs)
            
        return wrapper
    return decorator


def has_mod_permission():
    """
    Decorator to check if the user has moderator permissions.
    
    This works on both traditional commands and application commands.
    Moderator is defined as having either kick_members, ban_members,
    manage_messages, or administrator permissions.
    
    Returns:
        Command decorator
    """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Determine if this is a traditional command or application command
            if args and hasattr(args[0], 'bot'):
                # Traditional command
                cog = args[0]
                # For traditional commands, context is the second arg
                if len(args) > 1 and isinstance(args[1], commands.Context):
                    ctx = args[1]
                    
                    # Skip check in DMs (no guild)
                    if ctx and not ctx.guild:
                        await ctx.send("This command can only be used in a server.")
                        return
                    
                    # Check if user has mod permissions
                    permissions = ctx.author.guild_permissions
                    is_mod = (permissions.administrator or 
                             permissions.kick_members or 
                             permissions.ban_members or 
                             permissions.manage_messages)
                    
                    if not is_mod:
                        await ctx.send("You need moderator permissions to use this command.")
                        return
                    
                    return await func(*args, **kwargs)
                    
            elif args and hasattr(args[0], 'client'):
                # Application command
                cog = args[0]
                # For app commands, interaction is the second arg
                if len(args) > 1 and isinstance(args[1], discord.Interaction):
                    interaction = args[1]
                    
                    # Skip check in DMs (no guild)
                    if interaction and not interaction.guild:
                        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
                        return
                    
                    # Check if user has mod permissions
                    member = interaction.guild.get_member(interaction.user.id)
                    if not member:
                        await interaction.response.send_message("Could not find your permissions. Please try again.", ephemeral=True)
                        return
                        
                    permissions = member.guild_permissions
                    is_mod = (permissions.administrator or 
                             permissions.kick_members or 
                             permissions.ban_members or 
                             permissions.manage_messages)
                    
                    if not is_mod:
                        await interaction.response.send_message("You need moderator permissions to use this command.", ephemeral=True)
                        return
                    
                    return await func(*args, **kwargs)
            
            # If we can't determine the command type or context, just run the command
            return await func(*args, **kwargs)
            
        return wrapper
    return decorator


def check_server_limit():
    """
    Decorator to check if the guild has reached its server limit.
    
    For both traditional commands and application commands.
    
    Returns:
        Command decorator
    """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Determine if this is a traditional command or application command
            cog = None
            guild_id = None
            error_callback = None
            db = None
                
            if args and hasattr(args[0], 'bot'):
                # Traditional command
                cog = args[0]
                if len(args) > 1 and isinstance(args[1], commands.Context):
                    ctx = args[1]
                    # Skip check in DMs (no guild)
                    if ctx and not ctx.guild:
                        return await func(*args, **kwargs)
                    guild_id = str(ctx.guild.id)
                    db = cog.bot.db if hasattr(cog.bot, 'db') else None
                    # Error callback defined as a separate function to allow await
                    async def send_error(msg):
                        await ctx.send(msg)
                    error_callback = send_error
                    
            elif args and hasattr(args[0], 'client'):
                # Application command
                cog = args[0]
                if len(args) > 1 and isinstance(args[1], discord.Interaction):
                    interaction = args[1]
                    # Skip check in DMs (no guild)
                    if interaction and not interaction.guild:
                        return await func(*args, **kwargs)
                    guild_id = str(interaction.guild_id)
                    db = cog.client.db if hasattr(cog.client, 'db') else None
                    # Error callback defined as a separate function to allow await
                    async def send_error(msg):
                        await interaction.response.send_message(msg, ephemeral=True)
                    error_callback = send_error
            
            # If we can't determine the command type or context, just run the command
            if not guild_id or guild_id == "" or not db:
                return await func(*args, **kwargs)
                
            # Check server limit
            guild_model = await Guild.get_by_id(db, guild_id)
            has_capacity, error_message = await validate_server_limit(guild_model)
            
            if not has_capacity:
                if error_message and error_callback:
                    await error_callback(error_message)
                return
                
            return await func(*args, **kwargs)
            
        return wrapper
    return decorator


def command_handler(
    premium_feature: Optional[str] = None, 
    server_id_param: Optional[str] = None,
    check_server_limits: bool = False,
    guild_only_command: bool = True,
    cooldown_seconds: Optional[int] = None,
    error_messages: Optional[Dict[str, str]] = None,
    timeout_seconds: int = 10,
    retry_count: int = 2,
    log_metrics: bool = True,
    validate_parameters: bool = True
):
    """
    Enhanced command decorator that combines multiple validations and error handling.
    
    This comprehensive decorator provides bulletproof command handling with:
    1. Guild-only enforcement
    2. Premium feature validation
    3. Server ID validation and guild isolation
    4. Server limit enforcement
    5. Command cooldowns and rate limiting
    6. Automatic error recovery and predictive error handling
    7. Command execution metrics and performance tracking
    8. Timeout protection with configurable retries
    9. Comprehensive logging
    
    Args:
        premium_feature: Optional premium feature to check
        server_id_param: Optional server ID parameter name to validate
        check_server_limits: Whether to check server limits
        guild_only_command: Whether command requires a guild context
        cooldown_seconds: Optional cooldown in seconds
        error_messages: Optional custom error messages
        timeout_seconds: Timeout for command execution in seconds
        retry_count: Number of times to retry on transient errors
        log_metrics: Whether to log command metrics
        validate_parameters: Whether to validate command parameters
        
    Returns:
        Command decorator
    """
    # Default error messages
    default_errors = {
        "dm_context": "This command can only be used in a server.",
        "cooldown": "Please wait {seconds} seconds before using this command again.",
        "guild_not_found": "Server not set up. Please run `/setup` first.",
        "database_error": "Database connection error. Please try again later.",
        "timeout": "Command timed out. Please try again.",
        "unknown_error": "An error occurred while processing the command."
    }
    
    # Combine default and custom error messages
    messages = default_errors.copy()
    if error_messages:
        messages.update(error_messages)
    
    def decorator(func: CommandT) -> CommandT:
        command_name = func.__name__
        
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            
            # Initialize tracking for this command if needed
            if command_name not in COMMAND_METRICS:
                COMMAND_METRICS[command_name] = {
                    "invocations": 0,
                    "errors": 0,
                    "avg_runtime": 0,
                    "last_error": None,
                    "success_rate": 1.0
                }
                
            # Increment invocation counter
            COMMAND_METRICS[command_name]["invocations"] += 1
            
            # Extract command context
            ctx = None
            interaction = None
            user_id = None
            guild_id = None
            db = None
            bot = None
            
            # Determine command type and extract context
            is_traditional = False
            is_app_command = False
            
            if args and hasattr(args[0], 'bot'):
                # Traditional command
                cog = args[0]
                bot = cog.bot
                if len(args) > 1 and isinstance(args[1], commands.Context):
                    ctx = args[1]
                    is_traditional = True
                    user_id = ctx.author.id if ctx.author else None
                    guild_id = ctx.guild.id if ctx.guild else None
                    db = bot.db if hasattr(bot, 'db') else None
                    
            elif args and hasattr(args[0], 'client'):
                # Application command
                cog = args[0]
                bot = cog.client
                if len(args) > 1 and isinstance(args[1], discord.Interaction):
                    interaction = args[1]
                    is_app_command = True
                    user_id = interaction.user.id if interaction.user else None
                    guild_id = interaction.guild_id
                    db = bot.db if hasattr(bot, 'db') else None
                    
            # If we can't determine the command type or context, just run the command
            if not (is_traditional or is_app_command):
                try:
                    result = await func(*args, **kwargs)
                    return result
                except Exception as e:
                    logger.error(f"Error in {command_name}: {e}")
                    traceback.print_exc()
                    return None
                    
            # Helper function to send error messages
            async def send_error(message: str):
                try:
                    if is_traditional and ctx:
                        await ctx.send(message)
                    elif is_app_command and interaction:
                        # Check if interaction is already responded to
                        if interaction and interaction.response:
                            if not interaction.response.is_done():
                                await interaction.response.send_message(message, ephemeral=True)
                            else:
                                await interaction.followup.send(message, ephemeral=True)
                except Exception as e:
                    logger.error(f"Error sending command error message: {e}")
                    
            # 1. Check if we're in a guild (if required)
            if guild_only_command and not guild_id:
                await send_error(messages["dm_context"])
                return None
                
            # 2. Apply cooldown if specified
            if cooldown_seconds and user_id:
                user_key = f"{user_id}:{command_name}"
                now = time.time()
                
                if user_key in COMMAND_COOLDOWNS:
                    last_use = COMMAND_COOLDOWNS[user_key]
                    time_diff = now - last_use
                    
                    if time_diff < cooldown_seconds:
                        remaining = int(cooldown_seconds - time_diff)
                        cooldown_msg = messages["cooldown"].format(seconds=remaining)
                        await send_error(cooldown_msg)
                        return None
                        
                # Update cooldown timestamp
                COMMAND_COOLDOWNS[user_key] = now
                
            # Skip remaining checks if no guild (already passed guild_only check)
            if not guild_id or guild_id == "":
                try:
                    result = await func(*args, **kwargs)
                    
                    # Update metrics
                    runtime = time.time() - start_time
                    metrics = COMMAND_METRICS[command_name]
                    metrics["avg_runtime"] = (metrics["avg_runtime"] * (metrics["invocations"] - 1) + runtime) / metrics["invocations"]
                    
                    return result
                except Exception as e:
                    # Track error
                    COMMAND_METRICS[command_name]["errors"] += 1
                    COMMAND_METRICS[command_name]["last_error"] = str(e)
                    COMMAND_METRICS[command_name]["success_rate"] = (
                        (COMMAND_METRICS[command_name]["invocations"] - COMMAND_METRICS[command_name]["errors"]) / 
                        max(1, COMMAND_METRICS[command_name]["invocations"])
                    )
                    
                    logger.error(f"Error in command {command_name}: {e}")
                    await send_error(messages["unknown_error"])
                    return None
            
            # 3. Get guild model (needed for all remaining checks)
            guild_model = None
            if db:
                try:
                    # Use cached guild model if available
                    cache_key = f"guild:{guild_id}"
                    guild_model = await guild_cache.get(cache_key)
                    
                    if not guild_model:
                        guild_model = await Guild.get_by_id(db, guild_id)
                        if guild_model:
                            await guild_cache.set(cache_key, guild_model)
                except Exception as e:
                    logger.error(f"Database error getting guild model: {e}")
                    await send_error(messages["database_error"])
                    return None
            
            # Enhanced handling for premium validation
            if premium_feature:
                # First check if we have a guild model for checking features
                if guild_model:
                    # If guild_model exists, use the normal validation flow
                    has_access, error_message = await validate_premium_feature(guild_model, premium_feature)
                    if not has_access:
                        if error_message:
                            await send_error(error_message)
                        return None
                else:
                    # If guild_model doesn't exist, check premium tier directly
                    # This allows premium access without requiring complete guild setup
                    # Get the minimum tier required for this feature
                    from utils.premium import get_minimum_tier_for_feature
                    required_tier = get_minimum_tier_for_feature(premium_feature)
                    
                    if required_tier:
                        # Directly check premium tier access with the DB
                        has_access, error_message = await check_tier_access(db, guild_id, required_tier)
                        
                        if has_access:
                            # Continue with execution if premium tier is sufficient
                            logger.info(f"Guild {guild_id} has premium access to {premium_feature} without guild model")
                            
                            # Check if this is a server-related feature requiring guild model
                            guild_only_features = {
                                "economy", "gambling", "enhanced_economy", "premium_leaderboards",
                                "custom_embeds", "advanced_statistics"
                            }
                            
                            # For guild-only premium features, we can continue without a server
                            if premium_feature in guild_only_features:
                                # For purely guild-level premium features, we can proceed
                                pass
                            else:
                                # For server-dependent features, we should create a guild model
                                try:
                                    # Try to create guild model on-the-fly
                                    guild_model = await Guild.get_or_create(db, guild_id)
                                    if not guild_model:
                                        # If still can't create guild, show setup message for server features
                                        await send_error(messages["guild_not_found"])
                                        return None
                                except Exception as e:
                                    logger.error(f"Error creating guild model on-the-fly: {e}")
                                    await send_error(messages["guild_not_found"])
                                    return None
                        else:
                            # Premium check failed, return error message
                            if error_message:
                                await send_error(error_message)
                            return None
                    else:
                        # Feature not found in any tier, show guild setup message
                        await send_error(messages["guild_not_found"])
                        return None
            elif not guild_model:
                # No premium check but guild model required - show standard error
                await send_error(messages["guild_not_found"])
                return None
            
            # 5. Check server limits
            if check_server_limits:
                has_capacity, error_message = await validate_server_limit(guild_model)
                if not has_capacity:
                    if error_message:
                        await send_error(error_message)
                    return None
            
            # 6. Validate server ID if specified
            if server_id_param:
                # Get server_id from kwargs
                server_id = kwargs.get(server_id_param)
                if not server_id or server_id == "":
                    # If server_id not in kwargs, check command args
                    for i, arg in enumerate(args):
                        # Skip self/cog
                        if i == 0:
                            continue
                        # Skip context/interaction
                        if i == 1 and isinstance(arg, (commands.Context, discord.Interaction)):
                            continue
                        # Assume next positional arg might be server_id
                        if isinstance(arg, (str, int)):
                            server_id = str(arg)
                            break
                
                if server_id:
                    # Standardize server ID format
                    server_id = standardize_server_id(server_id)
                    
                    # Validate server format
                    if not validate_server_id_format(server_id):
                        await send_error(f"Invalid server ID format: {server_id}")
                        return None
                        
                    # Validate server exists and belongs to this guild
                    try:
                        # Check guild isolation
                        isolation_valid = await enforce_guild_isolation(db, server_id, guild_id)
                        if not isolation_valid:
                            await send_error(f"Server '{server_id}' does not belong to this Discord server.")
                            return None
                            
                        # Check server existence
                        server = await get_server_safely(db, server_id, guild_id)
                        if not server:
                            await send_error(f"Server '{server_id}' not found. Use `/list_servers` to see available servers.")
                            return None
                    except Exception as e:
                        logger.error(f"Error validating server {server_id}: {e}")
                        await send_error(f"Error validating server: {str(e)}")
                        return None
            
            # All checks passed, run the command with error handling and timeout protection
            retry_attempts = 0
            last_error = None
            transient_errors = (asyncio.TimeoutError, ConnectionError, OSError)
            
            # Track if we're about to execute a command that's been problematic
            is_problematic = False
            if command_name in COMMAND_METRICS:
                if COMMAND_METRICS[command_name]["invocations"] > 5:
                    success_rate = COMMAND_METRICS[command_name]["success_rate"]
                    if success_rate < 0.75:  # Less than 75% success rate
                        is_problematic = True
                        logger.warning(f"Executing problematic command {command_name} with historical success rate of {success_rate:.1%}")
            
            while retry_attempts <= retry_count:
                try:
                    # Use timeout for the command if specified
                    if timeout_seconds > 0:
                        async with asyncio.timeout(timeout_seconds):
                            # Add an informational message for retries
                            if retry_attempts > 0:
                                logger.info(f"Retry attempt {retry_attempts}/{retry_count} for command {command_name}")
                                # For problematic commands with retries, inform the user
                                if is_problematic and is_app_command and interaction and interaction.response and not interaction.response.is_done():
                                    await interaction.response.defer(ephemeral=True, thinking=True)
                            
                            # Execute the command
                            result = await func(*args, **kwargs)
                    else:
                        # Execute without timeout
                        result = await func(*args, **kwargs)
                    
                    # Command succeeded, update metrics
                    runtime = time.time() - start_time
                    metrics = COMMAND_METRICS[command_name]
                    metrics["avg_runtime"] = (metrics["avg_runtime"] * (metrics["invocations"] - 1) + runtime) / metrics["invocations"]
                    metrics["success_rate"] = (
                        (metrics["invocations"] - metrics["errors"]) / 
                        max(1, metrics["invocations"])
                    )
                    
                    # Log metrics if enabled
                    if log_metrics and metrics["invocations"] % 10 == 0:  # Log every 10 invocations
                        logger.info(
                            f"Command {command_name} metrics: "
                            f"{metrics['invocations']} invocations, "
                            f"{metrics['errors']} errors, "
                            f"{metrics['success_rate']:.1%} success rate, "
                            f"{metrics['avg_runtime']:.3f}s avg runtime"
                        )
                    
                    return result
                    
                except asyncio.TimeoutError as e:
                    last_error = e
                    retry_attempts += 1
                    logger.warning(f"Command {command_name} timed out (attempt {retry_attempts}/{retry_count+1})")
                    
                    # If this is the last retry, report the error
                    if retry_attempts > retry_count:
                        COMMAND_METRICS[command_name]["errors"] += 1
                        COMMAND_METRICS[command_name]["last_error"] = "Command timed out"
                        COMMAND_METRICS[command_name]["success_rate"] = (
                            (COMMAND_METRICS[command_name]["invocations"] - COMMAND_METRICS[command_name]["errors"]) / 
                            max(1, COMMAND_METRICS[command_name]["invocations"])
                        )
                        
                        logger.error(f"Command {command_name} timed out after {retry_count+1} attempts")
                        await send_error(f"{messages['timeout']} (after {retry_count+1} attempts)")
                        return None
                    
                    # Otherwise wait briefly before retry
                    await asyncio.sleep(0.5 * retry_attempts)  # Progressive backoff
                    
                except (ConnectionError, OSError) as e:
                    # These are network-related errors that might be transient
                    last_error = e
                    retry_attempts += 1
                    logger.warning(f"Network error in command {command_name}: {e} (attempt {retry_attempts}/{retry_count+1})")
                    
                    # If this is the last retry, report the error
                    if retry_attempts > retry_count:
                        COMMAND_METRICS[command_name]["errors"] += 1
                        COMMAND_METRICS[command_name]["last_error"] = f"Network error: {str(e)}"
                        COMMAND_METRICS[command_name]["success_rate"] = (
                            (COMMAND_METRICS[command_name]["invocations"] - COMMAND_METRICS[command_name]["errors"]) / 
                            max(1, COMMAND_METRICS[command_name]["invocations"])
                        )
                        
                        logger.error(f"Network error in command {command_name} after {retry_count+1} attempts: {e}")
                        await send_error(f"Network error occurred. Please try again later.")
                        return None
                    
                    # Otherwise wait briefly before retry
                    await asyncio.sleep(1.0 * retry_attempts)  # Progressive backoff
                    
                except Exception as e:
                    # Non-transient errors, don't retry
                    COMMAND_METRICS[command_name]["errors"] += 1
                    COMMAND_METRICS[command_name]["last_error"] = str(e)
                    COMMAND_METRICS[command_name]["success_rate"] = (
                        (COMMAND_METRICS[command_name]["invocations"] - COMMAND_METRICS[command_name]["errors"]) / 
                        max(1, COMMAND_METRICS[command_name]["invocations"])
                    )
                    
                    error_details = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
                    logger.error(f"Error in command {command_name}: {e}\n{error_details}")
                    
                    # Analyze error patterns to provide better user feedback
                    user_message = f"{messages['unknown_error']}"
                    
                    if "not found" in str(e).lower() or "does not exist" in str(e).lower():
                        user_message = f"The requested item could not be found. Please check your inputs and try again."
                    elif "permission" in str(e).lower() or "access" in str(e).lower():
                        user_message = f"You don't have permission to use this command or access this resource."
                    elif "already exists" in str(e).lower() or "duplicate" in str(e).lower():
                        user_message = f"This item already exists. Please use a different name or identifier."
                    elif "limit" in str(e).lower() or "exceeded" in str(e).lower() or "too many" in str(e).lower():
                        user_message = f"You've reached a limit for this action. Please try again later or contact an administrator."
                    elif "invalid" in str(e).lower() or "format" in str(e).lower():
                        user_message = f"One or more values you provided are invalid. Please check your inputs and try again."
                    elif "discord" in str(e).lower() and "api" in str(e).lower():
                        user_message = f"Discord API error occurred. Please try again later."
                    elif "database" in str(e).lower() or "mongo" in str(e).lower():
                        user_message = f"Database operation failed. Please try again later."
                    else:
                        # Include error details for unexpected errors
                        user_message = f"{messages['unknown_error']} Error: {str(e)}"
                    
                    # Send the error message to the user
                    await send_error(user_message)
                    return None
                
        # Update wrapper attributes for introspection
        wrapper.premium_feature = premium_feature
        wrapper.server_id_param = server_id_param
        wrapper.check_server_limits = check_server_limits
        wrapper.guild_only_command = guild_only_command
        wrapper.cooldown_seconds = cooldown_seconds
        wrapper.timeout_seconds = timeout_seconds
        wrapper.retry_count = retry_count
        wrapper.log_metrics = log_metrics
        wrapper.validate_parameters = validate_parameters
        
        return cast(CommandT, wrapper)
    
    return decorator
    
    
def get_command_metrics() -> Dict[str, Dict[str, Any]]:
    """
    Get all command metrics.
    
    Returns:
        Dict[str, Dict[str, Any]]: Mapping of command names to metrics
    """
    return COMMAND_METRICS


def get_problematic_commands() -> List[Dict[str, Any]]:
    """
    Get commands with high error rates.
    
    Returns:
        List[Dict[str, Any]]: List of problematic commands with their metrics
    """
    problematic = []
    
    for cmd_name, metrics in COMMAND_METRICS.items():
        # Only consider commands with enough invocations
        if metrics["invocations"] < ERROR_COUNT_THRESHOLD:
            continue
            
        # Check error rate
        if metrics["success_rate"] < (1.0 - HIGH_ERROR_THRESHOLD):
            problematic.append({
                "name": cmd_name,
                "error_rate": 1.0 - metrics["success_rate"],
                "invocations": metrics["invocations"],
                "errors": metrics["errors"],
                "last_error": metrics["last_error"],
                "avg_runtime": metrics["avg_runtime"]
            })
            
    # Sort by error rate (highest first)
    return sorted(problematic, key=lambda x: x["error_rate"], reverse=True)


def get_slow_commands() -> List[Dict[str, Any]]:
    """
    Get commands with slow execution times.
    
    Returns:
        List[Dict[str, Any]]: List of slow commands with their metrics
    """
    slow_commands = []
    
    for cmd_name, metrics in COMMAND_METRICS.items():
        # Only consider commands with enough invocations
        if metrics["invocations"] < ERROR_COUNT_THRESHOLD:
            continue
            
        # Check runtime
        if metrics["avg_runtime"] > SLOW_COMMAND_THRESHOLD:
            slow_commands.append({
                "name": cmd_name,
                "avg_runtime": metrics["avg_runtime"],
                "invocations": metrics["invocations"],
                "success_rate": metrics["success_rate"]
            })
            
    # Sort by runtime (slowest first)
    sorted_commands = sorted(slow_commands, key=lambda x: x["avg_runtime"], reverse=True)
    
    # Only return the top N slow commands
    return sorted_commands[:MAX_SLOW_COMMANDS]


def generate_command_metrics_report() -> str:
    """
    Generate a human-readable report of command metrics.
    
    Returns:
        str: Formatted report
    """
    if not COMMAND_METRICS:
        return "No command metrics collected yet."
        
    # Calculate overall stats
    total_invocations = sum(m["invocations"] for m in COMMAND_METRICS.values())
    total_errors = sum(m["errors"] for m in COMMAND_METRICS.values())
    avg_success_rate = sum(m["success_rate"] for m in COMMAND_METRICS.values()) / len(COMMAND_METRICS)
    
    # Get problematic and slow commands
    problematic = get_problematic_commands()
    slow = get_slow_commands()
    
    # Build report
    lines = [
        "📊 **Command Metrics Report**",
        f"Total Commands: {len(COMMAND_METRICS)}",
        f"Total Invocations: {total_invocations}",
        f"Overall Success Rate: {avg_success_rate:.2%}",
        f"Total Errors: {total_errors}",
        ""
    ]
    
    # Top commands by usage
    top_commands = sorted(
        [(name, m["invocations"]) for name, m in COMMAND_METRICS.items()],
        key=lambda x: x[1],
        reverse=True
    )[:5]
    
    if top_commands:
        lines.append("**Top Commands by Usage:**")
        for i, (name, count) in enumerate(top_commands, 1):
            lines.append(f"{i}. `{name}`: {count} invocations")
        lines.append("")
    
    # Problematic commands
    if problematic:
        lines.append("⚠️ **Problematic Commands:**")
        for i, cmd in enumerate(problematic, 1):
            lines.append(f"{i}. `{cmd['name']}`: {(1.0 - cmd['success_rate']):.2%} error rate ({cmd['errors']}/{cmd['invocations']})")
            if cmd['last_error']:
                lines.append(f"   Last error: {cmd['last_error']}")
        lines.append("")
    
    # Slow commands
    if slow:
        lines.append("🐢 **Slow Commands:**")
        for i, cmd in enumerate(slow, 1):
            lines.append(f"{i}. `{cmd['name']}`: {cmd['avg_runtime']:.2f}s avg runtime ({cmd['invocations']} invocations)")
        lines.append("")
    
    return "\n".join(lines)


def reset_command_metrics():
    """Reset all command metrics."""
    COMMAND_METRICS.clear()
    COMMAND_COOLDOWNS.clear()
    ERROR_TRACKING.clear()


def guild_only():
    """
    Decorator to ensure the command is only used in a guild.
    
    This works for both traditional commands and application commands.
    
    Returns:
        Command decorator
    """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Determine if this is a traditional command or application command
            if args and hasattr(args[0], 'bot'):
                # Traditional command
                if len(args) > 1 and isinstance(args[1], commands.Context):
                    ctx = args[1]
                    if ctx and not ctx.guild:
                        await ctx.send("This command can only be used in a server.")
                        return
                    return await func(*args, **kwargs)
                    
            elif args and hasattr(args[0], 'client'):
                # Application command
                if len(args) > 1 and isinstance(args[1], discord.Interaction):
                    interaction = args[1]
                    if interaction and not interaction.guild:
                        await interaction.response.send_message(
                            "This command can only be used in a server.", ephemeral=True
                        )
                        return
                    return await func(*args, **kwargs)
                    
            # If we can't determine the command type or context, just run the command
            return await func(*args, **kwargs)
            
        return wrapper
    return decorator