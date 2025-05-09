"""
Script to remove the test_server_123 from the database to stop SFTP connection issues
"""
import asyncio
import os
import logging
from utils.database import DatabaseManager
from models.server import Server

# Set up logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def remove_test_server():
    """Remove test_server_123 from the database"""
    logger.info("Connecting to database...")
    db = DatabaseManager()
    await db.initialize()
    
    logger.info("Looking for test_server_123 in the database...")
    
    # Check in servers collection - this is where CSV processor looks for SFTP servers
    logger.info("Checking servers collection...")
    test_server = await db.servers.find_one({"server_id": "test_server_123"})
    
    if test_server:
        logger.info(f"Found test server in servers collection: {test_server.get('server_id')} in guild {test_server.get('guild_id')}")
        logger.info(f"SFTP details: {test_server.get('sftp_host')}:{test_server.get('sftp_port')}")
        
        # Delete the server
        await db.servers.delete_one({"server_id": "test_server_123"})
        logger.info("Test server removed from servers collection successfully!")
    else:
        logger.info("No test_server_123 found in servers collection.")
    
    # Also check game_servers collection
    logger.info("Checking game_servers collection...")
    game_server = await db.game_servers.find_one({"server_id": "test_server_123"})
    
    if game_server:
        logger.info(f"Found test server in game_servers collection: {game_server.get('server_id')} in guild {game_server.get('guild_id')}")
        
        # Delete the server
        await db.game_servers.delete_one({"server_id": "test_server_123"})
        logger.info("Test server removed from game_servers collection successfully!")
    else:
        logger.info("No test_server_123 found in game_servers collection.")
    
    logger.info("Checking if any other test servers exist...")
    
    # Find any test servers in servers collection
    test_servers = await db.servers.find({"server_id": {"$regex": "test", "$options": "i"}}).to_list(length=100)
    
    if test_servers and len(test_servers) > 0:
        logger.info(f"Found {len(test_servers)} test servers in servers collection:")
        for server in test_servers:
            server_id = server.get('server_id')
            guild_id = server.get('guild_id')
            logger.info(f"  - {server_id} (guild: {guild_id})")
            await db.servers.delete_one({"server_id": server_id})
            logger.info(f"  → Removed {server_id} from servers collection")
    else:
        logger.info("No other test servers found in servers collection.")
    
    # Find any test servers in game_servers collection
    game_test_servers = await db.game_servers.find({"server_id": {"$regex": "test", "$options": "i"}}).to_list(length=100)
    
    if game_test_servers and len(game_test_servers) > 0:
        logger.info(f"Found {len(game_test_servers)} test servers in game_servers collection:")
        for server in game_test_servers:
            server_id = server.get('server_id')
            guild_id = server.get('guild_id')
            logger.info(f"  - {server_id} (guild: {guild_id})")
            await db.game_servers.delete_one({"server_id": server_id})
            logger.info(f"  → Removed {server_id} from game_servers collection")
    else:
        logger.info("No other test servers found in game_servers collection.")
        
    # Also search for test servers in guilds collection
    logger.info("Checking for test servers in guilds collection...")
    guilds_with_test = await db.guilds.find({"servers.server_id": {"$regex": "test", "$options": "i"}}).to_list(length=100)
    
    if guilds_with_test and len(guilds_with_test) > 0:
        logger.info(f"Found {len(guilds_with_test)} guilds with test servers:")
        for guild in guilds_with_test:
            guild_id = guild.get('guild_id')
            logger.info(f"Found guild {guild_id} with test servers")
            
            # Remove test servers from the servers array
            result = await db.guilds.update_one(
                {"guild_id": guild_id},
                {"$pull": {"servers": {"server_id": {"$regex": "test", "$options": "i"}}}}
            )
            logger.info(f"Removed test servers from guild {guild_id} ({result.modified_count} updates)")
    else:
        logger.info("No guilds with test servers found.")

def main():
    """Main entry point"""
    asyncio.run(remove_test_server())

if __name__ == "__main__":
    main()