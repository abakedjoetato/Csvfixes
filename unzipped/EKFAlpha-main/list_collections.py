"""
List all collections in the MongoDB database.
"""
import asyncio
import os
import motor.motor_asyncio

async def list_collections():
    """List all collections in the database"""
    # Connect to MongoDB
    mongodb_uri = os.environ.get('MONGODB_URI', '')
    if not mongodb_uri:
        print("MONGODB_URI environment variable not set")
        return
    
    client = motor.motor_asyncio.AsyncIOMotorClient(mongodb_uri)
    db = client['tower_of_temptation']  # Use the specific database name
    
    # List collections
    collections = await db.list_collection_names()
    print("Collections in MongoDB database 'tower_of_temptation':")
    if not collections:
        print("No collections found.")
    
    for collection in collections:
        print(f"- {collection}")
    
    # Count documents in each collection
    if collections:
        print("\nDocument counts:")
        for collection in collections:
            count = await db[collection].count_documents({})
            print(f"  {collection}: {count} documents")
    
    print(f"\nTotal: {len(collections)} collections")

if __name__ == "__main__":
    asyncio.run(list_collections())