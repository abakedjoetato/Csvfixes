"""
Simple script to check which discord library is active
"""
import sys
import os
import inspect
import importlib

print("=== Checking which discord library is active ===")
print(f"Python executable: {sys.executable}")
print(f"Python version: {sys.version}")

try:
    # Import discord carefully
    import discord
    print(f"Successfully imported discord, version: {discord.__version__}")
    print(f"Discord module location: {inspect.getfile(discord)}")
    
    # Check discord file structure without importing bridge directly
    discord_dir = os.path.dirname(inspect.getfile(discord))
    bridge_path = os.path.join(discord_dir, 'ext', 'bridge')
    bridge_exists = os.path.exists(bridge_path)
    print(f"Bridge directory exists: {bridge_exists}")
    
    # Check discord ext commands without trying bridge
    try:
        from discord.ext import commands
        print(f"Successfully imported discord.ext.commands")
        has_slash_command = hasattr(commands, 'slash_command')
        print(f"Has slash_command attribute: {has_slash_command}")
    except ImportError as e:
        print(f"Failed to import discord.ext.commands: {e}")
    
    # Check app_commands imports
    try:
        # Try with normal import
        from discord import app_commands
        print("Successfully imported discord.app_commands")
        
        # Check for AppCommandOptionType using safer methods
        has_option_type = hasattr(app_commands, 'AppCommandOptionType')
        print(f"app_commands has AppCommandOptionType: {has_option_type}")
    except ImportError as e:
        print(f"Failed to import discord.app_commands: {e}")
    
    # Try to determine if it's py-cord based on structure
    is_pycord = False
    if bridge_exists:
        is_pycord = True
    
    if has_slash_command:
        is_pycord = True
        
    # Check for commands.Context vs commands.context_managers
    if hasattr(commands, 'Context') and not hasattr(commands, 'context_managers'):
        # This is typical of discord.py
        print("Has discord.py-style commands.Context structure")
        
    if os.path.exists(os.path.join(discord_dir, 'commands')):
        print("Has discord/commands directory (may indicate py-cord)")
        is_pycord = True
    
    # Try to inspect app_commands location
    if 'app_commands' in dir(discord):
        app_commands_file = inspect.getfile(discord.app_commands)
        print(f"app_commands location: {app_commands_file}")
    
    # Final determination
    if is_pycord:
        print("\nCONCLUSION: py-cord appears to be active")
    else:
        print("\nCONCLUSION: discord.py appears to be active")
        
except ImportError as e:
    print(f"Failed to import discord: {e}")