"""
Script to add a test server to the database for debugging SFTP connections
"""
import asyncio
import logging
from utils.database import get_db
from models.guild import Guild
from models.server import Server

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("add_test_server")

async def add_test_server():
    logger.info("Adding test server to database...")
    
    # Get database connection
    db = await get_db()
    
    # Get or create guild
    guild_id = "1219706687980568769"  # The guild ID from logs
    guild_name = "Emerald Servers"  
    
    guild = await Guild.get_by_id(db, guild_id)
    if not guild:
        logger.info(f"Creating guild {guild_name} with ID {guild_id}")
        guild = Guild(db, {
            "guild_id": guild_id,
            "name": guild_name,
            "premium_tier": 4,  # Highest tier from logs
            "premium_expires": None  # Never expires for test
        })
        await guild.save()
    else:
        logger.info(f"Found existing guild: {guild.name} (ID: {guild.guild_id})")
    
    # Create test server with SFTP enabled - with both parameter sets for backward compatibility
    server_data = {
        "server_id": "test_server_123",
        "server_name": "Test SFTP Server",
        "guild_id": guild_id,
        "sftp_enabled": True,
        # Keep original parameter names for backward compatibility
        "sftp_host": "localhost",  # Use local SFTP server for testing
        "sftp_port": 22,
        "sftp_username": "test_user",
        "sftp_password": "test_password",
        "sftp_path": "/logs",
        # Add the new parameter mappings expected by SFTPManager
        "hostname": "localhost",
        "port": 22,
        "username": "test_user",
        "password": "test_password"
    }
    
    # Check if server already exists
    existing_server = await db.servers.find_one({"server_id": server_data["server_id"]})
    if existing_server:
        logger.info(f"Server already exists with ID {server_data['server_id']}, updating...")
        await db.servers.update_one(
            {"server_id": server_data["server_id"]},
            {"$set": server_data}
        )
    else:
        logger.info(f"Creating new server with ID {server_data['server_id']}")
        await db.servers.insert_one(server_data)
    
    # Verify server exists
    server = await db.servers.find_one({"server_id": server_data["server_id"]})
    if server:
        logger.info(f"Successfully saved server: {server.get('server_name')} (ID: {server.get('server_id')})")
        logger.info(f"SFTP details: Host: {server.get('sftp_host')}, Enabled: {server.get('sftp_enabled')}")
    else:
        logger.error(f"Failed to save server with ID {server_data['server_id']}")

    # List all servers in database
    all_servers = await db.servers.find({}).to_list(length=100)
    logger.info(f"Total servers in database after operation: {len(all_servers)}")
    for s in all_servers:
        logger.info(f"Server: {s.get('server_name')}, ID: {s.get('server_id')}, SFTP Enabled: {s.get('sftp_enabled', False)}")

if __name__ == "__main__":
    asyncio.run(add_test_server())