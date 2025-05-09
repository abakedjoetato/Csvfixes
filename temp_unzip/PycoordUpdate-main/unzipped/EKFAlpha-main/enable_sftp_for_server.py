"""
Script to enable SFTP for the existing server
"""
import asyncio
import logging
from utils.database import DatabaseManager

# Set up logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def enable_sftp_for_server():
    """Enable SFTP for the Emerald EU server in guild Emerald Servers"""
    logger.info("Connecting to database...")
    db = DatabaseManager()
    await db.initialize()

    guild_id = "1219706687980568769"  # Emerald Servers
    server_id = "7020"  # Emerald EU

    # 1. Update the server in the guild collection
    logger.info(f"Updating server {server_id} in guild {guild_id}...")
    guild_result = await db.guilds.update_one(
        {"guild_id": guild_id, "servers.server_id": server_id},
        {"$set": {
            "servers.$.sftp_enabled": False,
            "servers.$.sftp_host": "",
            "servers.$.sftp_port": 22,
            "servers.$.sftp_username": "",
            "servers.$.sftp_password": "",
            "servers.$.sftp_path": "/logs"
        }}
    )

    logger.info(f"Updated server in guild collection: {guild_result.modified_count} documents modified")

    # 2. Add a dedicated server entry in the servers collection (used by CSV/Log processors)
    logger.info(f"Adding server to servers collection...")

    # Check if server already exists in servers collection
    existing_server = await db.servers.find_one({"server_id": server_id})

    if existing_server:
        logger.info(f"Server {server_id} already exists in servers collection, updating...")
        server_result = await db.servers.update_one(
            {"server_id": server_id},
            {"$set": {
                "server_name": "Emerald EU",
                "guild_id": guild_id,
                "sftp_enabled": False, # Changed to False to reflect removal of SFTP
                "sftp_host": "",
                "sftp_port": 22,
                "sftp_username": "",
                "sftp_password": "",
                "sftp_path": "/logs",
                "csv_pattern": r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}\.csv"
            }}
        )
        logger.info(f"Updated server in servers collection: {server_result.modified_count} documents modified")
    else:
        logger.info(f"Server {server_id} not found in servers collection, creating new entry...")
        server_data = {
            "server_id": server_id,
            "server_name": "Emerald EU",
            "guild_id": guild_id,
            "sftp_enabled": False, # Changed to False to reflect removal of SFTP
            "sftp_host": "",
            "sftp_port": 22,
            "sftp_username": "",
            "sftp_password": "",
            "sftp_path": "/logs",
            "csv_pattern": r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}\.csv"
        }
        result = await db.servers.insert_one(server_data)
        logger.info(f"Created server in servers collection with _id: {result.inserted_id}")

    # 3. Verify the server was added/updated correctly
    logger.info("Verifying server configuration...")

    # Check guild collection
    guild = await db.guilds.find_one({"guild_id": guild_id})
    if guild:
        servers = guild.get("servers", [])
        for server in servers:
            if server.get("server_id") == server_id:
                logger.info(f"Server in guild collection: {server.get('server_name')} (ID: {server_id})")
                logger.info(f"  SFTP Enabled: {server.get('sftp_enabled')}")
                logger.info(f"  SFTP Host: {server.get('sftp_host')}:{server.get('sftp_port')}")

    # Check servers collection
    server = await db.servers.find_one({"server_id": server_id})
    if server:
        logger.info(f"Server in servers collection: {server.get('server_name')} (ID: {server_id})")
        logger.info(f"  SFTP Enabled: {server.get('sftp_enabled')}")
        logger.info(f"  SFTP Host: {server.get('sftp_host')}:{server.get('sftp_port')}")

def main():
    """Main entry point"""
    asyncio.run(enable_sftp_for_server())

if __name__ == "__main__":
    main()