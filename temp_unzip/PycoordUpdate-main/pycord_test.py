#!/usr/bin/env python3
"""
Test script to explore py-cord 2.6.1 module structure
"""
import os
import sys
import inspect
import importlib

# Try imports from py-cord 2.6.1
try:
    import discord
    print(f"Discord version: {getattr(discord, '__version__', 'Not available')}")
    print(f"Discord path: {getattr(discord, '__path__', 'Not available')}")
    
    # Check for commands module
    if hasattr(discord, 'commands'):
        print("discord.commands exists")
    else:
        print("discord.commands does not exist as an attribute")
    
    # Try importing from commands directory
    try:
        import discord.commands
        print("Successfully imported discord.commands")
        print(f"Commands path: {discord.commands.__path__ if hasattr(discord.commands, '__path__') else 'No path attribute'}")
        print("Commands module contents:")
        for name, obj in inspect.getmembers(discord.commands):
            if not name.startswith('_'):
                print(f"  {name}: {type(obj)}")
    except ImportError as e:
        print(f"Error importing discord.commands: {e}")
    
    # Try importing from ext.commands
    try:
        from discord.ext import commands
        print("Successfully imported discord.ext.commands")
        print("Commands module contents:")
        for name, obj in inspect.getmembers(commands):
            if not name.startswith('_'):
                print(f"  {name}: {type(obj)}")
    except ImportError as e:
        print(f"Error importing discord.ext.commands: {e}")
    
    # Try importing Bot from different locations
    try:
        from discord.ext.commands import Bot
        print("Successfully imported Bot from discord.ext.commands")
    except ImportError as e:
        print(f"Error importing Bot from discord.ext.commands: {e}")
    
    try:
        from discord.bot import Bot as DiscordBot
        print("Successfully imported Bot from discord.bot")
    except ImportError as e:
        print(f"Error importing Bot from discord.bot: {e}")
        
    # Try importing app_commands
    try:
        from discord import app_commands
        print("Successfully imported discord.app_commands")
        print("app_commands module contents:")
        for name, obj in inspect.getmembers(app_commands):
            if not name.startswith('_'):
                print(f"  {name}: {type(obj)}")
    except ImportError as e:
        print(f"Error importing discord.app_commands: {e}")
    
except ImportError as e:
    print(f"Error importing discord: {e}")