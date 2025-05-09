"""
Simple script to set home guild ID in the database for testing
"""
import os
import asyncio
import motor.motor_asyncio

async def set_home_guild(guild_id):
    """Set home guild ID in the database"""
    # Connect to MongoDB
    mongo_uri = os.environ.get('MONGODB_URI')
    if not mongo_uri:
        print("MONGODB_URI environment variable not set")
        return
    
    client = motor.motor_asyncio.AsyncIOMotorClient(mongo_uri)
    db = client.tower_of_temptation
    
    # Upsert to bot_config collection
    await db.bot_config.update_one(
        {"key": "home_guild_id"}, 
        {"$set": {"key": "home_guild_id", "value": str(guild_id)}},
        upsert=True
    )
    print(f"Home guild ID {guild_id} set in database")
    
    # Also set in environment
    os.environ["HOME_GUILD_ID"] = str(guild_id)
    print(f"Home guild ID {guild_id} set in environment")
    
    # Verify
    config = await db.bot_config.find_one({"key": "home_guild_id"})
    print("Home guild ID in database:", config)

if __name__ == "__main__":
    guild_id = 1219706687980568769  # Example guild ID (Emerald Servers)
    asyncio.run(set_home_guild(guild_id))