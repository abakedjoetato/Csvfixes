"""
Test script to verify py-cord 2.6.1 is working correctly and identify its features
"""
import importlib
import sys
import inspect

# Try to import discord (py-cord)
try:
    import discord
    from discord.ext import commands
    from discord import app_commands
    
    print(f"Successfully imported discord module (py-cord)")
    print(f"Version: {discord.__version__}")
    
    # Check if this is py-cord by looking for specific attributes
    is_pycord = hasattr(discord.ext, 'bridge')
    print(f"Is py-cord: {is_pycord}")
    
    # Print important classes and their locations
    print("\nKey modules and classes:")
    print(f"discord.__file__: {discord.__file__}")
    print(f"commands.__file__: {commands.__file__}")
    print(f"app_commands.__file__: {app_commands.__file__}")
    
    # Check for app_commands.AppCommandOptionType
    if hasattr(app_commands, 'AppCommandOptionType'):
        print("\napp_commands.AppCommandOptionType is available")
        option_type = app_commands.AppCommandOptionType
        for name, value in inspect.getmembers(option_type):
            if not name.startswith('_'):
                print(f"  {name} = {value}")
    else:
        print("\napp_commands.AppCommandOptionType not found")
        
        # Look for it in discord.enums
        if hasattr(discord, 'enums') and hasattr(discord.enums, 'AppCommandOptionType'):
            print("Found AppCommandOptionType in discord.enums instead")
            option_type = discord.enums.AppCommandOptionType
            for name, value in inspect.getmembers(option_type):
                if not name.startswith('_'):
                    print(f"  {name} = {value}")
    
    # Check for app_commands.Choice
    if hasattr(app_commands, 'Choice'):
        print("\napp_commands.Choice is available")
        print(f"  {app_commands.Choice}")
    else:
        print("\napp_commands.Choice not found")
    
    # Check for bridge support (py-cord specific)
    if hasattr(discord.ext, 'bridge'):
        print("\nbridge module is available (confirms py-cord)")
        from discord.ext import bridge
        print(f"  bridge.__file__: {bridge.__file__}")
        print(f"  bridge.Bot: {bridge.Bot}")
    
    # Check for tree sync method
    bot = commands.Bot(command_prefix='!', intents=discord.Intents.default())
    print("\nChecking Bot.tree.sync method:")
    print(f"  bot.tree: {bot.tree}")
    tree_methods = [method for method in dir(bot.tree) if not method.startswith('_')]
    print(f"  Tree methods: {tree_methods}")

except ImportError as e:
    print(f"Error importing discord: {e}")
    print("Please install py-cord with: pip install py-cord==2.6.1")
    sys.exit(1)

# List all installed packages with 'discord' in the name
print("\nInstalled packages with 'discord' in the name:")
for dist in importlib.metadata.distributions():
    name = dist.metadata["Name"]
    if "discord" in name.lower():
        print(f"  {name} {dist.version}")