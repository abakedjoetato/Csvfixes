
"""
Script to remove all servers from the database
"""
import asyncio
import logging
from utils.database import DatabaseManager

logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def remove_all_servers():
    """Remove all servers from all collections"""
    logger.info("Connecting to database...")
    db = DatabaseManager()
    await db.initialize()
    
    # Remove from game_servers collection
    result = await db.game_servers.delete_many({})
    logger.info(f"Removed {result.deleted_count} servers from game_servers collection")
    
    # Remove from servers collection
    result = await db.servers.delete_many({})
    logger.info(f"Removed {result.deleted_count} servers from servers collection")
    
    # Remove servers from guilds
    result = await db.guilds.update_many(
        {},
        {
            "$set": {
                "servers": [],
                "updated_at": datetime.utcnow()
            }
        }
    )
    logger.info(f"Cleared servers from {result.modified_count} guilds")
    
    logger.info("All servers have been removed from the database")

if __name__ == "__main__":
    asyncio.run(remove_all_servers())
