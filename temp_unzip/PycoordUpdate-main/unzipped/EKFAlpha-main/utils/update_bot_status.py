"""
Script to update the bot's status in the MongoDB database.

This script is meant to be run periodically to provide status updates about the Discord bot
and can be used for monitoring purposes.
"""
import os
import sys
import logging
import time
from datetime import datetime
import asyncio
import discord
import motor.motor_asyncio
from pymongo import errors as mongo_errors

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bot_status_updater.log')
    ]
)
logger = logging.getLogger(__name__)

# MongoDB connection
async def get_database():
    """Connect to MongoDB database"""
    mongodb_uri = os.environ.get("MONGODB_URI")
    if not mongodb_uri:
        logger.error("MONGODB_URI environment variable not set")
        return None
        
    try:
        # Create client with configuration
        client = motor.motor_asyncio.AsyncIOMotorClient(
            mongodb_uri,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=10000,
            socketTimeoutMS=45000,
            maxPoolSize=100,
            retryWrites=True
        )
        
        # Verify connection
        await client.admin.command('ping')
        
        # Get database
        db_name = os.environ.get("MONGODB_DB", "tower_of_temptation")
        db = client[db_name]
        logger.info(f"Connected to MongoDB database: {db_name}")
        
        return db
    except (mongo_errors.ConnectionFailure, mongo_errors.ServerSelectionTimeoutError) as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        return None

async def get_bot_client():
    """
    Create a Discord client and connect it to get bot status.
    """
    intents = discord.Intents.default()
    intents.guilds = True
    client = discord.Client(intents=intents)
    
    # Store values to be accessible in the on_ready event
    status_values = {
        'guild_count': 0,
        'is_connected': False,
        'start_time': time.time()
    }
    
    @client.event
    async def on_ready():
        logger.info(f'Logged in as {client.user.name} (ID: {client.user.id})')
        status_values['guild_count'] = len(client.guilds)
        status_values['is_connected'] = True
    
    # Start the client
    try:
        token = os.environ.get('DISCORD_TOKEN')
        if not token:
            logger.error('DISCORD_TOKEN environment variable not set')
            return None, status_values
        
        # We need to create a task and cancel it when we have the info
        # since we don't want to keep the bot running
        task = asyncio.create_task(client.start(token))
        
        # Wait a bit for the on_ready event
        timeout = 30  # seconds
        start_time = time.time()
        while not status_values['is_connected'] and time.time() - start_time < timeout:
            await asyncio.sleep(1)
        
        # Once we're ready or timed out, close the connection
        await client.close()
        task.cancel()
        
        return client, status_values
    except Exception as e:
        logger.error(f'Error connecting to Discord: {e}')
        return None, status_values

async def update_status():
    """Update the bot status in the MongoDB database."""
    try:
        # Connect to MongoDB
        db = await get_database()
        if not db:
            logger.error("Could not connect to MongoDB database")
            return False
            
        logger.info('Updating bot status...')
        
        # Connect to Discord to get real data
        client, status_values = await get_bot_client()
        
        # Create a new status entry
        uptime = 0
        if status_values['is_connected']:
            uptime = int(time.time() - status_values['start_time'])
        
        # Get version from environment or use default
        version = os.environ.get('BOT_VERSION', '0.1.0')
        
        status_data = {
            'timestamp': datetime.utcnow(),
            'is_online': status_values['is_connected'],
            'uptime_seconds': uptime,
            'guild_count': status_values['guild_count'],
            'version': version
        }
        
        # Add to database
        await db.bot_status.insert_one(status_data)
        logger.info(f'Status updated. Online: {status_data["is_online"]}, Guilds: {status_data["guild_count"]}')
        
        # If we have real data, update stats as well
        if status_values['is_connected']:
            # This would query MongoDB for current stats in a full implementation
            # For now it's just a placeholder
            stats_data = {
                'timestamp': datetime.utcnow(),
                'commands_used': await db.command_logs.count_documents({}),
                'active_users': await db.active_users.count_documents({}),
                'kills_tracked': await db.kills.count_documents({}),
                'bounties_placed': await db.bounties.count_documents({'status': 'active'}),
                'bounties_claimed': await db.bounties.count_documents({'status': 'claimed'})
            }
            
            await db.stats_snapshots.insert_one(stats_data)
            logger.info('Stats snapshot created')
        
        return True
    except Exception as e:
        logger.error(f'Error updating status: {e}')
        
        # Log the error in the database
        try:
            if db:
                error_log = {
                    'timestamp': datetime.utcnow(),
                    'level': 'ERROR',
                    'source': 'status_updater',
                    'message': str(e),
                    'traceback': str(sys.exc_info())
                }
                await db.error_logs.insert_one(error_log)
        except Exception as db_error:
            logger.error(f'Error logging to database: {db_error}')
        
        return False

if __name__ == "__main__":
    logger.info('Bot status updater started')
    asyncio.run(update_status())
    logger.info('Bot status updater finished')