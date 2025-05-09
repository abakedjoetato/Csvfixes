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
    # Import bot after logging is configured
    from bot import bot, initialize_bot
    
    # Initialize bot and database
    logger.info("Initializing bot...")
    await initialize_bot()
    
    if not bot.db:
        logger.error("Database not initialized")
        return
    
    # List server configurations from all collections
    logger.info("Listing server configurations...")
    
    # From servers collection
    logger.info("Servers from 'servers' collection:")
    servers_count = await bot.db.servers.count_documents({})
    logger.info(f"Found {servers_count} servers")
    
    async for server in bot.db.servers.find({}):
        server_id = server.get("_id", "unknown")
        server_name = server.get("name", "Unknown")
        hostname = server.get("hostname", "Unknown")
        original_id = server.get("original_server_id", "Not set")
        logger.info(f"Server: {server_name} (ID: {server_id}, Hostname: {hostname}, Original ID: {original_id})")
    
    # From game_servers collection
    logger.info("\nServers from 'game_servers' collection:")
    game_servers_count = await bot.db.game_servers.count_documents({})
    logger.info(f"Found {game_servers_count} game servers")
    
    async for server in bot.db.game_servers.find({}):
        server_id = server.get("server_id", "unknown")
        server_name = server.get("name", "Unknown")
        hostname = server.get("hostname", "Unknown")
        original_id = server.get("original_server_id", "Not set")
        logger.info(f"Game Server: {server_name} (ID: {server_id}, Hostname: {hostname}, Original ID: {original_id})")
    
    # From guilds collection (embedded servers)
    logger.info("\nServers from 'guilds' collection (embedded):")
    guilds_count = await bot.db.guilds.count_documents({})
    logger.info(f"Found {guilds_count} guilds")
    
    async for guild in bot.db.guilds.find({}):
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
    
    # Look for Tower of Temptation server
    logger.info("\nLooking for Tower of Temptation server...")
    
    # Try new UUID
    tot_server = await bot.db.servers.find_one({"_id": "1056852d-05f9-4e5e-9e88-012c2870c042"})
    if tot_server:
        logger.info(f"Found Tower of Temptation server with new UUID: {tot_server.get('_id')}")
        logger.info(f"Name: {tot_server.get('name')}")
        logger.info(f"Hostname: {tot_server.get('hostname')}")
        logger.info(f"Original ID: {tot_server.get('original_server_id', 'Not set')}")
    else:
        # Try old UUID
        tot_server = await bot.db.servers.find_one({"_id": "1b1ab57e-8749-4a40-b7a1-b1073a5f24b3"})
        if tot_server:
            logger.info(f"Found Tower of Temptation server with old UUID: {tot_server.get('_id')}")
            logger.info(f"Name: {tot_server.get('name')}")
            logger.info(f"Hostname: {tot_server.get('hostname')}")
            logger.info(f"Original ID: {tot_server.get('original_server_id', 'Not set')}")
        else:
            # Try by name
            tot_server = await bot.db.servers.find_one({"name": {"$regex": "Tower.*Temptation", "$options": "i"}})
            if tot_server:
                logger.info(f"Found Tower of Temptation server by name: {tot_server.get('_id')}")
                logger.info(f"Name: {tot_server.get('name')}")
                logger.info(f"Hostname: {tot_server.get('hostname')}")
                logger.info(f"Original ID: {tot_server.get('original_server_id', 'Not set')}")
            else:
                logger.info("Tower of Temptation server not found in servers collection")
    
    logger.info("Server configurations listing complete")

if __name__ == "__main__":
    asyncio.run(main())