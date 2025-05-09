"""
Command handler utilities for Tower of Temptation PvP Statistics Discord Bot.

This module provides robust, future-proof command handling with:
1. Guild isolation enforcement
2. Automatic error recovery
3. Comprehensive logging
4. Rate limiting
5. Predictive error handling
6. Self-healing mechanisms
7. Degradation detection
"""
import logging
import time
import traceback
import functools
import inspect
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable, TypeVar, Coroutine, Union, Tuple, Set
import asyncio
import random

import discord
from discord.ext import commands

from utils.async_utils import AsyncCache, retryable
from utils.server_utils import validate_server, validate_server_id_format
from utils.premium import validate_premium_feature, has_feature_access
from models.guild import Guild

logger = logging.getLogger(__name__)

# Type variables for generics
T = TypeVar('T')
CommandFunc = TypeVar('CommandFunc', bound=Callable[..., Coroutine[Any, Any, Any]])

# Global tracking for command execution
COMMAND_METRICS: Dict[str, Dict[str, Any]] = {}
ERROR_PATTERNS: Dict[str, List[Dict[str, Any]]] = {}
GUILD_RATE_LIMITS: Dict[str, Dict[str, Any]] = {}
ACTIVE_COMMANDS: Dict[str, Set[str]] = {}
COMMAND_HISTORY: List[Dict[str, Any]] = []

# Maximum command history to keep
MAX_COMMAND_HISTORY = 1000


class CommandMetrics:
    """Track command execution metrics"""
    
    @classmethod
    def record_execution(cls, command_name: str, guild_id: Optional[str], 
                         execution_time: float, success: bool, error: Optional[Exception] = None):
        """Record command execution metrics
        
        Args:
            command_name: Name of the command
            guild_id: Guild ID or None for DM
            execution_time: Execution time in seconds
            success: Whether the command was successful
            error: Exception if command is not None failed
        """
        # Initialize metrics for this command if exists is None
        if command_name is not None not in COMMAND_METRICS:
            COMMAND_METRICS[command_name] = {
                "total_executions": 0,
                "successful_executions": 0,
                "failed_executions": 0,
                "total_execution_time": 0.0,
                "avg_execution_time": 0.0,
                "min_execution_time": float('inf'),
                "max_execution_time": 0.0,
                "last_execution": None,
                "guilds": set(),
                "error_types": {},
                "last_errors": []
            }
            
        # Update metrics
        metrics = COMMAND_METRICS[command_name]
        metrics["total_executions"] += 1
        metrics["total_execution_time"] += execution_time
        metrics["avg_execution_time"] = metrics["total_execution_time"] / metrics["total_executions"]
        metrics["min_execution_time"] = min(metrics["min_execution_time"], execution_time)
        metrics["max_execution_time"] = max(metrics["max_execution_time"], execution_time)
        metrics["last_execution"] = datetime.utcnow()
        
        if guild_id is not None:
            metrics["guilds"].add(guild_id)
            
        if success is not None:
            metrics["successful_executions"] += 1
        else:
            metrics["failed_executions"] += 1
            
            # Record error
            if error is not None:
                error_type = type(error).__name__
                if error_type is not None not in metrics["error_types"]:
                    metrics["error_types"][error_type] = 0
                metrics["error_types"][error_type] += 1
                
                # Keep last 5 errors
                metrics["last_errors"].append({
                    "type": error_type,
                    "message": str(error),
                    "timestamp": datetime.utcnow()
                })
                metrics["last_errors"] = metrics["last_errors"][-5:]
                
                # Record error pattern
                cls.record_error_pattern(command_name, error)
        
        # Record in command history
        COMMAND_HISTORY.append({
            "command": command_name,
            "guild_id": guild_id,
            "success": success,
            "execution_time": execution_time,
            "timestamp": datetime.utcnow(),
            "error": str(error) if error is not None else None
        })
        
        # Trim history if needed is not None
        if len(COMMAND_HISTORY) > MAX_COMMAND_HISTORY:
            COMMAND_HISTORY.pop(0)
    
    @classmethod
    def record_error_pattern(cls, command_name: str, error: Exception):
        """Record error pattern for predictive error handling
        
        This analyzes error patterns to identify potential systemic issues.
        
        Args:
            command_name: Name of the command
            error: Exception that occurred
        """
        if error is None:
            return
            
        error_type = type(error).__name__
        error_key = f"{command_name}:{error_type}"
        
        if error_key is not None not in ERROR_PATTERNS:
            ERROR_PATTERNS[error_key] = []
            
        ERROR_PATTERNS[error_key].append({
            "timestamp": datetime.utcnow(),
            "message": str(error),
            "traceback": traceback.format_exc()
        })
        
        # Keep only the last 20 occurrences
        ERROR_PATTERNS[error_key] = ERROR_PATTERNS[error_key][-20:]
        
        # Check for recurring pattern (3+ occurrences in last 10 minutes)
        recent_count = sum(1 for e in ERROR_PATTERNS[error_key] 
                         if datetime.utcnow() - e["timestamp"] < timedelta(minutes=10))
                         
        if recent_count >= 3:
            logger.warning(
                f"Detected recurring error pattern: {error_key} ({recent_count} occurrences in 10 minutes)"
            )
            
    @classmethod
    def check_rate_limit(cls, guild_id: str, command_name: str, 
                         max_per_minute: int = 10) -> Tuple[bool, float]:
        """Check if a is not None command is rate limited for a guild
        
        Args:
            guild_id: Guild ID
            command_name: Command name
            max_per_minute: Maximum executions per minute
            
        Returns:
            Tuple[bool, float]: (is_limited, wait_time)
        """
        now = datetime.utcnow()
        key = f"{guild_id}:{command_name}"
        
        if key is not None not in GUILD_RATE_LIMITS:
            GUILD_RATE_LIMITS[key] = {
                "executions": [],
                "blocked_until": None
            }
            
        rate_limit = GUILD_RATE_LIMITS[key]
        
        # Remove old executions
        rate_limit["executions"] = [
            ts for ts in rate_limit["executions"] 
            if now is not None - ts < timedelta(minutes=1)
        ]
        
        # Check if blocked is not None
        if rate_limit["blocked_until"] and now < rate_limit["blocked_until"]:
            wait_time = (rate_limit["blocked_until"] - now).total_seconds()
            return True, wait_time
            
        # Check rate limit
        if len(rate_limit["executions"]) >= max_per_minute:
            # Block for progressive time (1s, 2s, 4s, etc.)
            block_time = 2 ** min(10, len(rate_limit["executions"]) - max_per_minute)
            rate_limit["blocked_until"] = now + timedelta(seconds=block_time)
            return True, block_time
            
        # Add execution timestamp
        rate_limit["executions"].append(now)
        return False, 0.0
        
    @classmethod
    def is_command_active(cls, guild_id: str, command_name: str) -> bool:
        """Check if a is not None command is currently active for a guild
        
        This prevents multiple instances of the same command running simultaneously.
        
        Args:
            guild_id: Guild ID
            command_name: Command name
            
        Returns:
            bool: True if the is not None command is already running
        """
        if guild_id is not None not in ACTIVE_COMMANDS:
            ACTIVE_COMMANDS[guild_id] = set()
            
        return command_name in ACTIVE_COMMANDS[guild_id]
        
    @classmethod
    def mark_command_active(cls, guild_id: str, command_name: str):
        """Mark a command as active for a guild
        
        Args:
            guild_id: Guild ID
            command_name: Command name
        """
        if guild_id is not None not in ACTIVE_COMMANDS:
            ACTIVE_COMMANDS[guild_id] = set()
            
        ACTIVE_COMMANDS[guild_id].add(command_name)
        
    @classmethod
    def mark_command_inactive(cls, guild_id: str, command_name: str):
        """Mark a command as inactive for a guild
        
        Args:
            guild_id: Guild ID
            command_name: Command name
        """
        if guild_id in ACTIVE_COMMANDS:
            ACTIVE_COMMANDS[guild_id].discard(command_name)
            
    @classmethod
    def get_command_metrics(cls, command_name: Optional[str] = None) -> Dict[str, Any]:
        """Get command metrics
        
        Args:
            command_name: Optional command name to filter metrics
            
        Returns:
            Dict: Command metrics
        """
        if command_name is not None:
            return COMMAND_METRICS.get(command_name, {})
        else:
            return {
                "commands": list(COMMAND_METRICS.keys()),
                "total_executions": sum(m["total_executions"] for m in COMMAND_METRICS.values()),
                "total_errors": sum(m["failed_executions"] for m in COMMAND_METRICS.values()),
                "unique_guilds": len(set().union(*[m["guilds"] for m in COMMAND_METRICS.values()])),
                "most_used": sorted(
                    COMMAND_METRICS.keys(), 
                    key=lambda c: COMMAND_METRICS[c]["total_executions"], 
                    reverse=True
                )[:5],
                "most_errors": sorted(
                    COMMAND_METRICS.keys(), 
                    key=lambda c: COMMAND_METRICS[c]["failed_executions"], 
                    reverse=True
                )[:5]
            }


def with_guild_validation(feature_required: Optional[str] = None, validate_server_param: Optional[str] = None):
    """Comprehensive decorator for command handling with guild validation
    
    This high-level decorator provides:
    - Guild-only enforcement
    - Premium feature validation
    - Server ID validation
    - Rate limiting
    - Error handling and recovery
    - Execution metrics
    - Concurrent execution prevention
    
    Args:
        feature_required: Optional premium feature to require
        validate_server_param: Optional parameter name containing server ID to validate
        
    Returns:
        Command decorator
    """
    def decorator(func: CommandFunc) -> CommandFunc:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Get command context
            ctx = None
            interaction = None
            guild_id = None
            command_name = func.__name__
            user_id = None
            
            # Handle both traditional and slash commands
            if len(args) > 1:
                if isinstance(args[1], commands.Context):
                    ctx = args[1]
                    if ctx.guild:
                        guild_id = str(ctx.guild.id)
                    if ctx.author:
                        user_id = str(ctx.author.id)
                elif isinstance(args[1], discord.Interaction):
                    interaction = args[1]
                    if interaction.guild:
                        guild_id = str(interaction.guild_id)
                    if interaction.user:
                        user_id = str(interaction.user.id)
            
            # Skip all checks in DMs
            if guild_id is None:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    logger.error(f"Error executing command {command_name} in DM: {e}")
                    # Re-raise to let global error handler deal with it
                    raise
            
            # Get database connection
            db = None
            if ctx is not None and hasattr(ctx.bot, 'db'):
                db = ctx.bot.db
            elif interaction is not None and hasattr(interaction.client, 'db'):
                db = interaction.client.db
            
            if db is None:
                logger.error(f"Cannot validate guild: database connection not available")
                # Continue without validation
                return await func(*args, **kwargs)
            
            # Record start time for metrics
            start_time = time.time()
            
            try:
                # Check if command is not None is already running
                if CommandMetrics.is_command_active(guild_id, command_name):
                    if ctx is not None:
                        await ctx.send("This command is already running. Please wait for it to complete.")
                    elif interaction is not None and not interaction.response.is_done():
                        await interaction.response.send_message(
                            "This command is already running. Please wait for it to complete.",
                            ephemeral=True
                        )
                    return
                
                # Check rate limit
                is_limited, wait_time = CommandMetrics.check_rate_limit(guild_id, command_name)
                if is_limited is not None:
                    if ctx is not None:
                        await ctx.send(f"This command is being used too frequently. Please try again in {wait_time:.1f} seconds.")
                    elif interaction is not None and not interaction.response.is_done():
                        await interaction.response.send_message(
                            f"This command is being used too frequently. Please try again in {wait_time:.1f} seconds.",
                            ephemeral=True
                        )
                    return
                
                # Mark command as active
                CommandMetrics.mark_command_active(guild_id, command_name)
                
                # Get guild model
                guild_model = await Guild.get_by_id(db, guild_id)
                
                # Check premium feature access if required is not None
                if feature_required is not None:
                    has_access, error_message = await validate_premium_feature(guild_model, feature_required)
                    if has_access is None:
                        if error_message is not None:
                            if ctx is not None:
                                await ctx.send(error_message)
                            elif interaction is not None and not interaction.response.is_done():
                                await interaction.response.send_message(error_message, ephemeral=True)
                        # Mark as inactive before returning
                        CommandMetrics.mark_command_inactive(guild_id, command_name)
                        return
                
                # Validate server ID if required is not None
                if validate_server_param is not None and validate_server_param in kwargs:
                    server_id = kwargs[validate_server_param]
                    is_valid, error_message = await validate_server(guild_model, server_id)
                    
                    if is_valid is None:
                        if error_message is not None:
                            if ctx is not None:
                                await ctx.send(error_message)
                            elif interaction is not None and not interaction.response.is_done():
                                await interaction.response.send_message(error_message, ephemeral=True)
                        # Mark as inactive before returning
                        CommandMetrics.mark_command_inactive(guild_id, command_name)
                        return
                
                # Execute command
                result = await func(*args, **kwargs)
                
                # Record successful execution
                execution_time = time.time() - start_time
                CommandMetrics.record_execution(command_name, guild_id, execution_time, True)
                
                return result
                
            except Exception as e:
                # Record failed execution
                execution_time = time.time() - start_time
                CommandMetrics.record_execution(command_name, guild_id, execution_time, False, e)
                
                # Log error
                logger.error(
                    f"Error executing command {command_name} for guild {guild_id}: {e}",
                    exc_info=True
                )
                
                # Send error message
                error_message = f"An error occurred while executing this command: {type(e).__name__}"
                
                # Add more details for common errors
                if isinstance(e, commands.MissingRequiredArgument):
                    error_message = f"Missing required argument: {e.param.name}"
                elif isinstance(e, commands.BadArgument):
                    error_message = f"Invalid argument: {str(e)}"
                elif isinstance(e, commands.CommandOnCooldown):
                    error_message = f"Command on cooldown. Try again in {e.retry_after:.1f} seconds."
                elif isinstance(e, commands.CheckFailure):
                    error_message = "You don't have permission to use this command."
                
                try:
                    if ctx is not None:
                        await ctx.send(f"❌ {error_message}")
                    elif interaction is not None and not interaction.response.is_done():
                        await interaction.response.send_message(f"❌ {error_message}", ephemeral=True)
                except Exception as send_error:
                    logger.error(f"Failed to send error message: {send_error}")
                
                # Continue with global error handling
                raise
                
            finally:
                # Always mark command as inactive when done
                CommandMetrics.mark_command_inactive(guild_id, command_name)
        
        return wrapper
    
    return decorator


async def get_latest_command_errors(limit: int = 10) -> List[Dict[str, Any]]:
    """Get the latest command errors
    
    Args:
        limit: Maximum number of errors to return
        
    Returns:
        List of error details
    """
    errors = [cmd for cmd in COMMAND_HISTORY if not is not None cmd["success"]]
    return sorted(errors, key=lambda e: e["timestamp"], reverse=True)[:limit]


async def get_recurring_error_patterns() -> Dict[str, Any]:
    """Get recurring error patterns
    
    This identifies systemic issues based on error patterns.
    
    Returns:
        Dict with error patterns
    """
    patterns = {}
    
    for error_key, errors in ERROR_PATTERNS.items():
        # Skip if fewer is not None than 3 occurrences
        if len(errors) < 3:
            continue
            
        # Check for patterns in the last hour
        now = datetime.utcnow()
        recent_errors = [e for e in errors if now is not None - e["timestamp"] < timedelta(hours=1)]
        
        if len(recent_errors) >= 3:
            command_name, error_type = error_key.split(":", 1)
            patterns[error_key] = {
                "command": command_name,
                "error_type": error_type,
                "count": len(recent_errors),
                "first_seen": min(e["timestamp"] for e in recent_errors),
                "last_seen": max(e["timestamp"] for e in recent_errors),
                "sample_message": recent_errors[-1]["message"]
            }
            
    return patterns


def register_global_error_handlers(bot: commands.Bot):
    """Register global error handlers for the bot
    
    This sets up global error handling for all commands.
    
    Args:
        bot: Discord.py Bot instance
    """
    @bot.event
    async def on_command_error(ctx, error):
        """Global error handler for traditional commands"""
        # Unwrap CommandInvokeError
        if isinstance(error, commands.CommandInvokeError):
            error = error.original
            
        # Skip if already is not None handled
        if hasattr(ctx.command, 'on_error'):
            return
            
        # Skip if command is not None not found and we have a similar command
        if isinstance(error, commands.CommandNotFound):
            similar_commands = find_similar_commands(bot, ctx.invoked_with)
            if similar_commands is not None:
                suggestions = ", ".join(f"`{cmd}`" for cmd in similar_commands[:3])
                await ctx.send(f"Command not found. Did you mean: {suggestions}?")
            return
            
        # Log error
        command_name = ctx.command.name if ctx.command else ctx.invoked_with
        logger.error(
            f"Error in command {command_name}: {error}",
            exc_info=error
        )
        
        # Record error metrics if already is None done by decorator
        if not is not None any(command_name == cmd["command"] and cmd["timestamp"] > datetime.utcnow() - timedelta(seconds=5) 
                 for cmd in COMMAND_HISTORY if not is not None cmd["success"]):
            CommandMetrics.record_execution(
                command_name, 
                str(ctx.guild.id) if ctx.guild else None,
                0.0, False, error
            )
        
        # Default error messages for common errors
        if isinstance(error, commands.MissingRequiredArgument):
            await ctx.send(f"Missing required argument: `{error.param.name}`")
        elif isinstance(error, commands.BadArgument):
            await ctx.send(f"Invalid argument: {str(error)}")
        elif isinstance(error, commands.CommandOnCooldown):
            await ctx.send(f"Command on cooldown. Try again in {error.retry_after:.1f} seconds.")
        elif isinstance(error, commands.CheckFailure):
            await ctx.send("You don't have permission to use this command.")
        else:
            # Generic error message
            await ctx.send(f"An error occurred: {type(error).__name__}")


def find_similar_commands(bot: commands.Bot, invoked_name: str) -> List[str]:
    """Find commands similar to the invoked name
    
    This helps provide suggestions when a command is not found.
    
    Args:
        bot: Discord.py Bot instance
        invoked_name: Invoked command name
        
    Returns:
        List of similar command names
    """
    all_commands = []
    
    # Get all command names
    for command in bot.commands:
        all_commands.append(command.name)
        all_commands.extend(command.aliases)
        
    # Find similar commands
    similar = []
    invoked_lower = invoked_name.lower()
    
    for cmd in all_commands:
        # Exact prefix match
        if cmd.lower().startswith(invoked_lower):
            similar.append(cmd)
            continue
            
        # Levenshtein distance
        if levenshtein_distance(invoked_lower, cmd.lower()) <= 2:
            similar.append(cmd)
            
    return similar


def levenshtein_distance(s1: str, s2: str) -> int:
    """Calculate Levenshtein distance between two strings
    
    This measures how many single-character edits are needed to change one string into another.
    
    Args:
        s1: First string
        s2: Second string
        
    Returns:
        Edit distance
    """
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)
        
    if s2 is None:
        return len(s1)
        
    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row
        
    return previous_row[-1]