"""
Script to list all server configurations in the database
"""
import asyncio
import logging
from utils.database import DatabaseManager

logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def list_server_configs():
    """List all server configurations in the database"""
    logger.info("Connecting to database...")
    db = DatabaseManager()
    await db.initialize()
    
    # Check servers collection (used by CSV processor)
    logger.info("Listing servers in 'servers' collection:")
    servers_count = await db.servers.count_documents({})
    logger.info(f"Found {servers_count} servers in 'servers' collection")
    
    servers = await db.servers.find({}).to_list(length=100)
    for server in servers:
        server_id = server.get('server_id', 'Unknown')
        server_name = server.get('server_name', 'Unnamed')
        guild_id = server.get('guild_id', 'Unknown')
        sftp_enabled = server.get('sftp_enabled', False)
        sftp_host = server.get('sftp_host', 'None')
        sftp_port = server.get('sftp_port', 'None')
        
        logger.info(f"Server: {server_name} (ID: {server_id})")
        logger.info(f"  Guild ID: {guild_id}")
        logger.info(f"  SFTP Enabled: {sftp_enabled}")
        if sftp_enabled:
            logger.info(f"  SFTP Host: {sftp_host}:{sftp_port}")
        logger.info("---")
    
    # Check game_servers collection
    logger.info("\nListing servers in 'game_servers' collection:")
    game_servers_count = await db.game_servers.count_documents({})
    logger.info(f"Found {game_servers_count} servers in 'game_servers' collection")
    
    game_servers = await db.game_servers.find({}).to_list(length=100)
    for server in game_servers:
        server_id = server.get('server_id', 'Unknown')
        name = server.get('name', 'Unnamed')
        guild_id = server.get('guild_id', 'Unknown')
        sftp_host = server.get('sftp_host', 'None')
        sftp_port = server.get('sftp_port', 'None')
        
        logger.info(f"Game Server: {name} (ID: {server_id})")
        logger.info(f"  Guild ID: {guild_id}")
        logger.info(f"  SFTP Host: {sftp_host}:{sftp_port}")
        logger.info("---")
    
    # Check guilds collection for servers
    logger.info("\nListing servers in 'guilds' collection:")
    guilds = await db.guilds.find({}).to_list(length=100)
    for guild in guilds:
        guild_id = guild.get('guild_id', 'Unknown')
        guild_name = guild.get('name', 'Unknown Guild')
        servers = guild.get('servers', [])
        
        logger.info(f"Guild: {guild_name} (ID: {guild_id})")
        logger.info(f"  Premium Tier: {guild.get('premium_tier', 0)}")
        logger.info(f"  Server Count: {len(servers)}")
        
        for server in servers:
            server_id = server.get('server_id', 'Unknown')
            server_name = server.get('server_name', 'Unnamed')
            sftp_enabled = server.get('sftp_enabled', False)
            
            logger.info(f"  - Server: {server_name} (ID: {server_id})")
            logger.info(f"    SFTP Enabled: {sftp_enabled}")
            if sftp_enabled:
                logger.info(f"    SFTP Host: {server.get('sftp_host', 'None')}:{server.get('sftp_port', 'None')}")
        
        logger.info("---")

def main():
    """Main entry point"""
    asyncio.run(list_server_configs())

if __name__ == "__main__":
    main()