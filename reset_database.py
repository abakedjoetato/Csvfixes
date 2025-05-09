"""
Reset the MongoDB database to a clean state.

This script will:
1. Connect to the MongoDB database
2. Drop all collections
3. Create any necessary default data
"""
import os
import sys
import asyncio
import logging
from datetime import datetime
import motor.motor_asyncio

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('reset_database')

async def reset_database():
    """Reset the MongoDB database to a clean state"""
    # Connect to MongoDB
    mongodb_uri = os.environ.get('MONGODB_URI', '')
    if not mongodb_uri:
        logger.error("MONGODB_URI environment variable not set")
        return False
        
    logger.info("Connecting to MongoDB database...")
    client = motor.motor_asyncio.AsyncIOMotorClient(mongodb_uri)
    db = client['tower_of_temptation']  # Use the specific database name
    
    logger.info("Connected to database")
    
    # Get list of all collections
    collection_names = await db.list_collection_names()
    logger.info(f"Found {len(collection_names)} collections: {', '.join(collection_names)}")
    
    # Drop all collections
    for collection_name in collection_names:
        logger.info(f"Dropping collection: {collection_name}")
        await db[collection_name].drop()
    
    logger.info("All collections dropped")
    
    # Recreate database structure
    # The bot will automatically re-create all needed collections and indexes
    # as it starts up and processes commands
    
    logger.info("Database reset complete. The bot will automatically create necessary collections on startup.")
    return True

def main():
    """Main entry point"""
    print("=" * 60)
    print("WARNING: This will delete all data in the MongoDB database.")
    print("This action cannot be undone!")
    print("=" * 60)
    
    # In Replit environment, we'll automatically proceed with reset
    print("Automatic confirmation for Replit environment")
    print("Resetting database...")
    
    result = asyncio.run(reset_database())
    
    if result:
        print("Database reset completed successfully.")
        return 0
    else:
        print("Database reset failed.")
        return 1

if __name__ == "__main__":
    sys.exit(main())