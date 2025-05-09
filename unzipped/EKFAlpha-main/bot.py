"""
Tower of Temptation PvP Statistics Discord Bot
Main bot initialization and configuration
"""
import os
import sys
import asyncio
import logging
import discord
from discord.ext import commands
from utils.database import get_db
from models.guild import Guild
from utils.sftp import periodic_connection_maintenance

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bot.log')
    ]
)
logger = logging.getLogger('bot')

# Bot configuration
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.guilds = True

async def sync_guilds_with_database(bot):
    """
    Synchronize all current Discord guilds with the database.
    This ensures that guilds added while the bot was offline are properly registered.
    """
    if bot.db is None:
        logger.error("Cannot sync guilds: Database connection not established")
        return
    
    logger.info(f"Syncing {len(bot.guilds)} guilds with database...")
    
    for guild in bot.guilds:
        try:
            # Use get_or_create to ensure the guild exists in database
            # Creates the guild if it doesn't exist yet
            guild_model = await Guild.get_or_create(bot.db, guild.id, guild.name)
            if guild_model:
                logger.info(f"Synced guild: {guild.name} (ID: {guild.id})")
            else:
                logger.error(f"Failed to sync guild: {guild.name} (ID: {guild.id})")
        except Exception as e:
            logger.error(f"Error syncing guild {guild.name} (ID: {guild.id}): {e}")
    
    logger.info("Guild synchronization complete")

async def initialize_bot(force_sync=False):
    """Initialize the Discord bot and load cogs"""
    # Create bot instance with hardcoded owner ID
    bot = commands.Bot(
        command_prefix='!', 
        intents=intents, 
        help_command=None,
        owner_id=462961235382763520  # Correct hardcoded owner ID (constant truth)
    )
    
    # Initialize background tasks dictionary
    bot.background_tasks = {}
    
    # Initialize SFTP connections dictionary
    bot.sftp_connections = {}
    
    # Initialize database connection
    logger.info("Initializing database connection...")
    try:
        bot.db = await get_db()
        logger.info("Database connection established")
        
        # Initialize home guild ID from database
        bot.home_guild_id = None
        try:
            # Look for home_guild_id in a bot_config collection
            bot_config = await bot.db.bot_config.find_one({"key": "home_guild_id"})
            if bot_config and "value" in bot_config:
                bot.home_guild_id = int(bot_config["value"])
                logger.info(f"Retrieved home guild ID from database: {bot.home_guild_id}")
            else:
                # Try environment variable as fallback
                home_guild_id = os.environ.get('HOME_GUILD_ID')
                if home_guild_id:
                    try:
                        bot.home_guild_id = int(home_guild_id)
                        logger.info(f"Using home guild ID from environment: {bot.home_guild_id}")
                    except (ValueError, TypeError):
                        logger.warning("Invalid HOME_GUILD_ID in environment variables")
        except Exception as e:
            logger.error(f"Error loading home guild ID: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        bot.db = None
    
    # Bot events
    @bot.event
    async def on_ready():
        """Called when the bot is ready to start accepting commands"""
        logger.info(f"Bot logged in as {bot.user.name} (ID: {bot.user.id})")
        logger.info(f"Connected to {len(bot.guilds)} guilds")
        logger.info(f"Discord API version: {discord.__version__}")
        
        # Set bot status
        activity = discord.Activity(type=discord.ActivityType.watching, name="Tower of Temptation")
        await bot.change_presence(activity=activity)
        
        # Initialize guilds database records for all connected guilds
        # This ensures guilds added while bot was offline are properly registered
        await sync_guilds_with_database(bot)
        
        # Synchronize server data between collections
        # This ensures original_server_id is consistent across all collections
        if bot.db:
            logger.info("Synchronizing server data between collections...")
            try:
                await bot.db.synchronize_server_data()
                logger.info("Server data synchronization complete")
            except Exception as e:
                logger.error(f"Error during server data synchronization: {e}", exc_info=True)
        
        # Start SFTP connection maintenance task
        if 'sftp_maintenance' not in bot.background_tasks or bot.background_tasks['sftp_maintenance'].done():
            logger.info("Starting SFTP connection maintenance task")
            bot.background_tasks['sftp_maintenance'] = asyncio.create_task(
                periodic_connection_maintenance(interval=120)  # Run every 2 minutes
            )
            # Add error handler to prevent unhandled task exceptions
            bot.background_tasks['sftp_maintenance'].add_done_callback(
                lambda t: logger.error(f"SFTP maintenance task stopped: {t.exception()}") 
                if t.exception() else None
            )
        
        if force_sync:
            logger.info("Syncing application commands...")
            await bot.tree.sync()
            logger.info("Application commands synced!")
    
    @bot.event
    async def on_guild_join(guild):
        """Called when the bot joins a new guild"""
        logger.info(f"Bot joined new guild: {guild.name} (ID: {guild.id})")
        
        try:
            # Use get_or_create to ensure the guild exists in database
            # This creates a database record for the new guild automatically
            guild_model = await Guild.get_or_create(bot.db, guild.id, guild.name)
            if guild_model:
                logger.info(f"Created database record for new guild: {guild.name} (ID: {guild.id})")
                
                # Synchronize server data if a new guild is added
                # to ensure any imported servers have proper original_server_id values
                if bot.db:
                    try:
                        await bot.db.synchronize_server_data()
                        logger.info(f"Synchronized server data after adding guild {guild.name}")
                    except Exception as e:
                        logger.error(f"Error synchronizing server data after adding guild {guild.name}: {e}")
            else:
                logger.error(f"Failed to create database record for guild: {guild.name} (ID: {guild.id})")
        except Exception as e:
            logger.error(f"Error creating database record for guild {guild.name} (ID: {guild.id}): {e}")
    
    @bot.event
    async def on_command_error(ctx, error):
        """Global error handler for commands"""
        if isinstance(error, commands.CommandNotFound):
            return
        
        if isinstance(error, commands.MissingRequiredArgument):
            await ctx.send(f"Missing required argument: {error.param.name}")
            return
        
        if isinstance(error, commands.BadArgument):
            await ctx.send(f"Bad argument: {error}")
            return
        
        # For all other errors, log them
        logger.error(f"Error in command {ctx.command}: {error}")
        await ctx.send("An error occurred while executing the command. Please try again later.")
    
    # Load cogs
    logger.info("Loading cogs...")
    cog_count = 0
    cog_dir = 'cogs'
    for filename in os.listdir(cog_dir):
        if filename.endswith('.py') and not filename.startswith('_'):
            cog_name = filename[:-3]
            try:
                await bot.load_extension(f"{cog_dir}.{cog_name}")
                logger.info(f"Loaded cog: {cog_name}")
                cog_count += 1
            except Exception as e:
                logger.error(f"Failed to load cog {cog_name}: {e}")
    
    logger.info(f"Successfully loaded {cog_count} cogs")
    return bot

async def run_bot():
    """Run the Discord bot"""
    # Check for token
    token = os.environ.get('DISCORD_TOKEN')
    if not token:
        logger.error("DISCORD_TOKEN environment variable not set")
        return 1
    
    try:
        # Initialize the bot
        bot = await initialize_bot(force_sync=True)
        
        # Start the bot
        logger.info("Starting bot...")
        await bot.start(token)
    except discord.LoginFailure:
        logger.error("Invalid token provided")
        return 1
    except Exception as e:
        logger.error(f"Error starting bot: {e}")
        import traceback
        traceback.print_exc()
        return 1
    return 0

def main():
    """Main entry point"""
    return asyncio.run(run_bot())

if __name__ == "__main__":
    sys.exit(main())