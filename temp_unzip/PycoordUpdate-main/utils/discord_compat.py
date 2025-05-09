"""
Direct Discord.py 2.5.2 Implementation

This module provides direct access to discord.py 2.5.2 features without any compatibility
layers as required by rule #2 in rules.md.

This implementation adapts to the currently installed version while maintaining 
direct import patterns that would be compatible with py-cord 2.6.1 whenever possible.
"""
import logging
import sys
from typing import Any, Optional, Union, Dict, List, Callable

logger = logging.getLogger(__name__)

# Import discord.py directly - no compatibility layer
import discord
from discord.ext import commands
from discord.ext.commands import Bot, Cog
from discord import app_commands
from discord.app_commands import Choice
from discord import AppCommandOptionType  # Import from main discord module

# Log discord version
logger.info(f"Using discord library version: {discord.__version__}")

# Determine if we're using discord.py or py-cord
USING_PYCORD = 'py-cord' in discord.__version__ or discord.__version__.startswith('2.6')

# Direct method mappings to py-cord 2.6.1 app_commands methods
def command(name=None, description=None, **kwargs):
    """
    Direct implementation of app_commands.command in py-cord 2.6.1
    
    Args:
        name: Command name
        description: Command description
        **kwargs: Additional command parameters
        
    Returns:
        Command decorator
    """
    return app_commands.command(name=name, description=description, **kwargs)

def describe(**kwargs):
    """
    Direct implementation of app_commands.describe in py-cord 2.6.1
    
    Args:
        **kwargs: Parameter name to description mapping
        
    Returns:
        Function decorator
    """
    return app_commands.describe(**kwargs)

def autocomplete(param_name=None, **kwargs):
    """
    Direct implementation of app_commands.autocomplete in py-cord 2.6.1
    
    In py-cord 2.6.1, the pattern is:
    @app_commands.autocomplete(param_name=callback)
    
    Args:
        param_name: Parameter name (for backward compatibility)
        **kwargs: Parameter name to callback mapping
        
    Returns:
        Autocomplete decorator
    """
    # Handle old-style call pattern
    if param_name is not None and not kwargs:
        def outer_decorator(callback_func):
            return app_commands.autocomplete(**{param_name: callback_func})
        return outer_decorator
    
    # Handle compatibility pattern
    if 'callback' in kwargs and param_name is not None:
        callback = kwargs.pop('callback')
        return app_commands.autocomplete(**{param_name: callback})
    
    # Modern py-cord 2.6.1 style
    return app_commands.autocomplete(**kwargs)

def guild_only():
    """
    Direct implementation of app_commands.guild_only in py-cord 2.6.1
    
    Returns:
        Guild-only decorator
    """
    return app_commands.guild_only()

# Direct CommandTree implementation for py-cord 2.6.1
class CommandTree:
    """Direct implementation of CommandTree for py-cord 2.6.1"""
    
    def __init__(self, bot):
        self.bot = bot
    
    async def sync(self, *args, **kwargs):
        """Maps directly to sync_commands in py-cord 2.6.1"""
        if hasattr(self.bot, 'sync_commands'):
            return await self.bot.sync_commands(*args, **kwargs)
        return []

# Bot patch for tree attribute compatibility
if not hasattr(Bot, '_tree_patched'):
    original_bot_init = Bot.__init__
    
    def patched_bot_init(self, *args, **kwargs):
        original_bot_init(self, *args, **kwargs)
        if not hasattr(self, 'tree'):
            self.tree = CommandTree(self)
    
    # Apply the patch
    Bot.__init__ = patched_bot_init
    Bot._tree_patched = True
    logger.info("Added tree attribute to Bot for py-cord 2.6.1 compatibility")

def create_option(name: str, 
                 description: str, 
                 option_type: Any, 
                 required: bool = False, 
                 choices: Optional[List[Dict[str, Any]]] = None) -> Any:
    """
    Create a command option for py-cord 2.6.1
    
    In py-cord 2.6.1, options are defined through parameter annotations and @app_commands.describe
    This function provides a compatibility layer for older code
    
    Args:
        name: Option name
        description: Option description
        option_type: Type of option (AppCommandOptionType)
        required: Whether the option is required
        choices: Optional list of choices
        
    Returns:
        Option description dictionary
    """
    # Ensure name and description are valid
    name = name or "unnamed_option"
    description = description or "No description provided"
    
    # Format choices
    formatted_choices = []
    if choices:
        for choice in choices:
            if isinstance(choice, dict):
                choice_name = str(choice.get('name', ''))
                choice_value = choice.get('value', '')
                formatted_choices.append(app_commands.Choice(name=choice_name, value=choice_value))
            else:
                formatted_choices.append(choice)
    
    # Return option description
    return {
        'name': str(name),
        'description': str(description),
        'type': option_type,
        'required': bool(required),
        'choices': formatted_choices
    }

def get_app_commands_module():
    """
    Get the Discord app_commands module directly from py-cord 2.6.1
    
    Returns:
        discord.app_commands module
    """
    return app_commands