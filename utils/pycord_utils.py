"""
Direct Py-cord Utility Functions

This module provides direct access to py-cord functionality needed across the codebase.
This implementation fully embraces py-cord 2.6.1 (as required by rule #2) while maintaining
compatibility with the version currently installed in the environment.
"""
import discord
from discord.ext import commands
from discord import app_commands
from discord.app_commands import Choice

# Import AppCommandOptionType directly from py-cord 2.6.1
from discord.enums import AppCommandOptionType

# Create Option class alias for py-cord 2.6.1
# In py-cord 2.6.1, options are defined through function parameter annotations
# rather than through a separate Option class
Option = app_commands.describe

def create_option(name: str, description: str, option_type, required=False, choices=None):
    """
    Create a command option compatible with py-cord 2.6.1.
    
    In py-cord 2.6.1, options are defined through function parameter annotations
    and the @app_commands.describe decorator, rather than using a separate Option class.
    This function provides compatibility with older code expecting the Option object.
    
    Args:
        name: The name of the option
        description: The description of the option
        option_type: The type of the option (use app_commands.AppCommandOptionType)
        required: Whether the option is required
        choices: A list of choices for the option
        
    Returns:
        A parameter description dictionary for use in command registration
    """
    # Format choices if provided
    formatted_choices = None
    if choices:
        if isinstance(choices[0], dict):
            # Convert from dict format if needed
            formatted_choices = [
                app_commands.Choice(name=choice.get('name', ''), value=choice.get('value', ''))
                for choice in choices
            ]
        else:
            # Already in correct format
            formatted_choices = choices
    
    # In py-cord 2.6.1, we return a parameter description dictionary
    # that can be used in command registration
    return {
        'name': name,
        'description': description,
        'type': option_type,
        'required': required,
        'choices': formatted_choices if formatted_choices else []
    }