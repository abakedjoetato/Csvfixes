"""
Test script to determine the correct imports for py-cord 2.6.1
"""
import sys
import discord

print(f"Using Python {sys.version}")
print(f"Discord package path: {discord.__file__}")

# Try to find where commands module is located
import pkgutil
print("\nAvailable modules in discord package:")
for module in pkgutil.iter_modules(discord.__path__):
    print(f"- {module.name}")

print("\nTrying to import commands module...")
try:
    import discord.commands
    print("Successfully imported discord.commands")
    print(f"Commands module path: {discord.commands.__file__}")
except ImportError as e:
    print(f"Error importing discord.commands: {e}")

# Try to import bot module
print("\nTrying to import bot module...")
try:
    import discord.bot
    print("Successfully imported discord.bot")
    print(f"Bot module path: {discord.bot.__file__}")
    print("Bot module contains:")
    for item in dir(discord.bot):
        if not item.startswith('__'):
            print(f"- {item}")
except ImportError as e:
    print(f"Error importing discord.bot: {e}")

# Try to see what's in the app_commands module
print("\nTrying to import app_commands module...")
try:
    import discord.app_commands
    print("Successfully imported discord.app_commands")
    print(f"App commands module path: {discord.app_commands.__file__}")
    print("Important classes/functions in app_commands:")
    for item in dir(discord.app_commands):
        if not item.startswith('__') and not item.islower():
            print(f"- {item}")
except ImportError as e:
    print(f"Error importing discord.app_commands: {e}")

print("\nTrying to access Bot directly from discord...")
try:
    print(f"discord.Bot exists: {'Bot' in dir(discord)}")
    if 'Bot' in dir(discord):
        print(f"discord.Bot info: {discord.Bot}")
except Exception as e:
    print(f"Error accessing discord.Bot: {e}")