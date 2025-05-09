#!/usr/bin/env python3
"""List server configurations"""

import asyncio
import logging
import json
from datetime import datetime
from typing import Dict, Any, List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def main():
    """Main entry point"""
    # Import and initialize database directly
    from utils.database import DatabaseManager
    from utils.server_identity import get_server_id_mappings
    
    # Initialize database
    logger.info("Initializing database...")
    db_manager = DatabaseManager()
    await db_manager.initialize()
    
    if not db_manager.db:
        logger.error("Database not initialized")
        return
        
    # Get server mappings
    logger.info("Loading server mappings...")
    try:
        server_mappings = await get_server_id_mappings(db_manager.db)
        logger.info(f"Loaded {len(server_mappings)} server mappings:")
        for uuid, original_id in server_mappings.items():
            logger.info(f"  UUID: {uuid} -> Original ID: {original_id}")
    except Exception as e:
        logger.error(f"Error loading server mappings: {e}")
        server_mappings = {}
    
    # List server configurations from all collections
    logger.info("Listing server configurations...")
    
    db = db_manager.db
    
    # From servers collection
    logger.info("Servers from 'servers' collection:")
    servers_count = await db.servers.count_documents({})
    logger.info(f"Found {servers_count} servers")
    
    async for server in db.servers.find({}):
        server_id = server.get("_id", "unknown")
        server_name = server.get("name", "Unknown")
        hostname = server.get("hostname", "Unknown")
        original_id = server.get("original_server_id", "Not set")
        logger.info(f"Server: {server_name} (ID: {server_id}, Hostname: {hostname}, Original ID: {original_id})")
    
    # From game_servers collection
    logger.info("\nServers from 'game_servers' collection:")
    game_servers_count = await db.game_servers.count_documents({})
    logger.info(f"Found {game_servers_count} game servers")
    
    async for server in db.game_servers.find({}):
        server_id = server.get("server_id", "unknown")
        server_name = server.get("name", "Unknown")
        hostname = server.get("hostname", "Unknown")
        original_id = server.get("original_server_id", "Not set")
        logger.info(f"Game Server: {server_name} (ID: {server_id}, Hostname: {hostname}, Original ID: {original_id})")
    
    # From guilds collection (embedded servers)
    logger.info("\nServers from 'guilds' collection (embedded):")
    guilds_count = await db.guilds.count_documents({})
    logger.info(f"Found {guilds_count} guilds")
    
    async for guild in db.guilds.find({}):
        guild_id = guild.get("_id", "unknown")
        guild_name = guild.get("name", "Unknown")
        logger.info(f"Guild: {guild_name} (ID: {guild_id})")
        
        if "servers" in guild and guild["servers"]:
            logger.info(f"  Guild has {len(guild['servers'])} embedded servers")
            for server in guild["servers"]:
                server_id = server.get("server_id", "unknown")
                server_name = server.get("name", "Unknown")
                hostname = server.get("hostname", "Unknown")
                original_id = server.get("original_server_id", "Not set")
                sftp_enabled = server.get("sftp_enabled", False)
                logger.info(f"  Server: {server_name} (ID: {server_id}, Hostname: {hostname}, Original ID: {original_id}, SFTP: {sftp_enabled})")
        else:
            logger.info("  Guild has no embedded servers")
    
    # Look for Emeralds Killfeed server
    logger.info("\nLooking for Emeralds Killfeed servers...")
    
    emerald_servers = await db.servers.find({"original_server_id": "7020"}).to_list(10)
    if emerald_servers:
        logger.info(f"Found {len(emerald_servers)} Emeralds Killfeed servers with original ID 7020")
        for server in emerald_servers:
            logger.info(f"  UUID: {server.get('_id')}")
            logger.info(f"  Name: {server.get('name')}")
            logger.info(f"  Hostname: {server.get('hostname')}")
            logger.info(f"  Original ID: {server.get('original_server_id', 'Not set')}")
    else:
        # Try by name
        emerald_servers = await db.servers.find({"name": {"$regex": "Emerald", "$options": "i"}}).to_list(10)
        if emerald_servers:
            logger.info(f"Found {len(emerald_servers)} Emeralds servers by name")
            for server in emerald_servers:
                logger.info(f"  UUID: {server.get('_id')}")
                logger.info(f"  Name: {server.get('name')}")
                logger.info(f"  Hostname: {server.get('hostname')}")
                logger.info(f"  Original ID: {server.get('original_server_id', 'Not set')}")
        else:
            logger.info("No Emeralds servers found in servers collection")
    
    logger.info("Server configurations listing complete")

if __name__ == "__main__":
    asyncio.run(main())