import sys
import pkg_resources

print("Python version:", sys.version)
print("\nInstalled packages:")

for pkg in pkg_resources.working_set:
    if "discord" in pkg.key or "cord" in pkg.key:
        print(f"{pkg.key} = {pkg.version}")

try:
    import discord
    print("\nDiscord.py version:", discord.__version__)
    print("Discord.py location:", discord.__file__)
    
    if hasattr(discord, '__title__'):
        print("Discord library title:", discord.__title__)
    
    # Check for app_commands
    if hasattr(discord, 'app_commands'):
        print("app_commands is available")
    else:
        print("app_commands is NOT available")
        
    # Check for tree attribute
    from discord.ext.commands import Bot
    print("Bot class attributes related to commands:")
    bot_attrs = [attr for attr in dir(Bot) if not attr.startswith('_') and ('command' in attr or 'tree' in attr)]
    for attr in bot_attrs:
        print(f"- {attr}")
    
    # Check for AppCommandOptionType in discord.enums
    if hasattr(discord, 'enums'):
        enums_attrs = dir(discord.enums)
        print("\nAttributes in discord.enums:")
        for attr in enums_attrs:
            if 'Type' in attr:
                print(f"- {attr}")
    
except ImportError as e:
    print(f"Error importing discord: {e}")
except Exception as e:
    print(f"Error: {e}")