"""
Script to synchronize server data between collections.

This script ensures that all server collections (guilds.servers, servers, game_servers)
have consistent data, particularly the original_server_id which is crucial for path construction.

Usage:
    python sync_server_collections.py [server_id]
    
If server_id is provided, only that server is synchronized. Otherwise, all servers are synchronized.
"""
import asyncio
import logging
import sys
from utils.database import get_db

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('sync_collections.log')
    ]
)
logger = logging.getLogger('sync_collections')

async def sync_server_collections(server_id=None):
    """Synchronize server data between collections
    
    Args:
        server_id: Optional server ID to synchronize. If None, all servers are synchronized.
    """
    # Get database connection
    db = await get_db()
    if not db:
        logger.error("Failed to connect to database")
        return False
    
    try:
        logger.info(f"Starting server data synchronization{f' for server {server_id}' if server_id else ''}")
        
        # Call the database method to synchronize server data
        result = await db.synchronize_server_data(server_id)
        
        if result:
            logger.info("Server data synchronization completed successfully")
        else:
            logger.error("Server data synchronization failed")
            
        return result
    except Exception as e:
        logger.error(f"Error during server data synchronization: {e}")
        return False

async def main():
    """Main entry point"""
    # Check for server_id argument
    server_id = None
    if len(sys.argv) > 1:
        server_id = sys.argv[1]
        logger.info(f"Synchronizing server data for server ID: {server_id}")
    else:
        logger.info("Synchronizing server data for all servers")
    
    # Run the synchronization process
    result = await sync_server_collections(server_id)
    
    if result:
        logger.info("Server data synchronization completed successfully")
        return 0
    else:
        logger.error("Server data synchronization failed")
        return 1

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))