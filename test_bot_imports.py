"""
Test script to verify bot imports are working correctly
Testing direct imports from discord
"""
import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('test_bot_imports')

def main():
    """Test direct discord imports without compatibility layer"""
    try:
        # Directly import from discord/py-cord
        import discord
        from discord.ext import commands
        from discord.ext.commands import Bot
        from discord import app_commands
        
        # Print version
        logger.info(f"Discord library version: {discord.__version__}")
        
        # Print success message
        logger.info("Successfully imported discord modules:")
        logger.info(f"  - discord module path: {discord.__file__}")
        logger.info(f"  - commands module path: {commands.__file__}")
        
        # Check available attributes
        logger.info("\nVerifying core attributes:")
        logger.info(f"  - discord.Intents: {hasattr(discord, 'Intents')}")
        logger.info(f"  - discord.Activity: {hasattr(discord, 'Activity')}")
        logger.info(f"  - commands.Cog: {hasattr(commands, 'Cog')}")
        logger.info(f"  - app_commands.command: {hasattr(app_commands, 'command')}")
                
        return 0
    except Exception as e:
        logger.error(f"Error importing discord modules: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())