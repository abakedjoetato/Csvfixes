"""
Py-cord 2.6.1 adapter module

This module ensures the codebase uses py-cord 2.6.1 by acting as an adapter.
It handles imports properly and provides compatibility with the expected APIs.
"""
import sys
import os
import importlib.util
import importlib.metadata

# Verify package installation
try:
    pycord_dist = next(d for d in importlib.metadata.distributions() if d.metadata["Name"] == "py-cord")
    print(f"Using py-cord version: {pycord_dist.version}")
except StopIteration:
    print("py-cord not found. Please install it with: pip install py-cord==2.6.1")
    raise ImportError("py-cord is not installed")

# Import the essential modules
from discord import app_commands
from discord.ext import commands
from discord import ui
from discord import abc

# Check which version is active by testing features specific to py-cord
try:
    # This will work only in py-cord
    from discord.ext import bridge
    using_pycord = True
    print("Successfully imported py-cord specific modules")
except ImportError:
    using_pycord = False
    print("Warning: Using discord.py, not py-cord")

# Provide the app_commands.AppCommandOptionType enum
class AppCommandOptionType:
    """Application command option types for py-cord 2.6.1"""
    SUB_COMMAND = 1
    SUB_COMMAND_GROUP = 2
    STRING = 3
    INTEGER = 4
    BOOLEAN = 5
    USER = 6
    CHANNEL = 7
    ROLE = 8
    MENTIONABLE = 9
    NUMBER = 10  # FLOAT in discord.py
    ATTACHMENT = 11

# Make sure AppCommandOptionType is available in app_commands
if not hasattr(app_commands, 'AppCommandOptionType'):
    app_commands.AppCommandOptionType = AppCommandOptionType
    print("Added AppCommandOptionType to app_commands")

# Export all modules and types
__all__ = ['app_commands', 'commands', 'ui', 'abc', 'AppCommandOptionType', 'using_pycord']