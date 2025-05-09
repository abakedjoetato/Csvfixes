#!/usr/bin/env python
"""Script to check database structure"""

import asyncio
import json
import sys
import os
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def main():
    """Check database collections and structure"""
    try:
        # Import necessary modules
        from utils.database import DatabaseManager
        
        # Initialize database connection
        logger.info("Initializing database connection...")
        db = DatabaseManager()
        await db.initialize()
        logger.info("Database connection established")
        
        # Check guilds collection
        logger.info("Checking guilds collection...")
        guild_count = await db.guilds.count_documents({})
        guild = await db.guilds.find_one({})
        logger.info(f"Found {guild_count} guilds")
        if guild:
            # Print guild structure (omitting large arrays)
            guild_copy = guild.copy()
            for key, value in guild_copy.items():
                if isinstance(value, list) and len(value) > 5:
                    guild_copy[key] = f"[{len(value)} items]"
            logger.info(f"Guild document structure: {json.dumps(guild_copy, default=str, indent=2)}")
            
            # Check if guild has servers
            if 'servers' in guild and isinstance(guild['servers'], list):
                logger.info(f"Guild has {len(guild['servers'])} embedded servers")
                if guild['servers']:
                    logger.info(f"Sample embedded server: {json.dumps(guild['servers'][0], default=str, indent=2)}")
        
        # Check servers collection
        logger.info("Checking servers collection...")
        server_count = await db.servers.count_documents({})
        server = await db.servers.find_one({})
        logger.info(f"Found {server_count} servers")
        if server:
            logger.info(f"Server document structure: {json.dumps(server, default=str, indent=2)}")
        
        # Check game_servers collection
        logger.info("Checking game_servers collection...")
        game_server_count = await db.game_servers.count_documents({})
        game_server = await db.game_servers.find_one({})
        logger.info(f"Found {game_server_count} game servers")
        if game_server:
            logger.info(f"Game server document structure: {json.dumps(game_server, default=str, indent=2)}")
        
        # Check if any servers have the original_server_id field
        logger.info("Checking for original_server_id field in servers...")
        servers_with_original_id = await db.servers.count_documents({"original_server_id": {"$exists": True}})
        logger.info(f"Found {servers_with_original_id} servers with original_server_id field")
        
        # Check server IDs consistency
        logger.info("Checking server ID consistency across collections...")
        all_server_ids = set()
        all_server_names = {}
        
        # Get server IDs from servers collection
        async for server in db.servers.find({}, {"_id": 1, "name": 1}):
            server_id = server.get("_id")
            server_name = server.get("name", "Unknown")
            all_server_ids.add(server_id)
            all_server_names[server_id] = server_name
            
        # Get server IDs from game_servers collection
        async for game_server in db.game_servers.find({}, {"server_id": 1, "name": 1}):
            server_id = game_server.get("server_id")
            server_name = game_server.get("name", "Unknown")
            all_server_ids.add(server_id) 
            all_server_names[server_id] = server_name
            
        # Get server IDs from guilds collection (embedded servers)
        async for guild in db.guilds.find({}, {"servers": 1}):
            if "servers" in guild and isinstance(guild["servers"], list):
                for server in guild["servers"]:
                    if "server_id" in server:
                        server_id = server.get("server_id")
                        server_name = server.get("name", "Unknown")
                        all_server_ids.add(server_id)
                        all_server_names[server_id] = server_name
        
        logger.info(f"Found {len(all_server_ids)} unique server IDs across all collections")
        for server_id, server_name in all_server_names.items():
            logger.info(f"Server ID: {server_id}, Name: {server_name}")
            
        # Check specific server - Tower of Temptation
        logger.info("Checking Tower of Temptation server specifically...")
        tot_server = await db.servers.find_one({"_id": "1056852d-05f9-4e5e-9e88-012c2870c042"})
        if tot_server:
            logger.info(f"Found Tower of Temptation server: {json.dumps(tot_server, default=str, indent=2)}")
        else:
            # Try with the older UUID
            tot_server = await db.servers.find_one({"_id": "1b1ab57e-8749-4a40-b7a1-b1073a5f24b3"})
            if tot_server:
                logger.info(f"Found Tower of Temptation server with old UUID: {json.dumps(tot_server, default=str, indent=2)}")
            else:
                # Try searching by name
                tot_server = await db.servers.find_one({"name": {"$regex": "Tower.*Temptation", "$options": "i"}})
                if tot_server:
                    logger.info(f"Found Tower of Temptation server by name: {json.dumps(tot_server, default=str, indent=2)}")
                else:
                    logger.info("Tower of Temptation server not found in servers collection")
        
        # Check which guild the Tower of Temptation server belongs to
        logger.info("Checking which guild the Tower of Temptation server belongs to...")
        tot_guild = await db.guilds.find_one({"servers.server_id": "1056852d-05f9-4e5e-9e88-012c2870c042"})
        if tot_guild:
            logger.info(f"Tower of Temptation server belongs to guild: {tot_guild.get('name', tot_guild.get('_id'))}")
        else:
            # Try with the older UUID
            tot_guild = await db.guilds.find_one({"servers.server_id": "1b1ab57e-8749-4a40-b7a1-b1073a5f24b3"})
            if tot_guild:
                logger.info(f"Tower of Temptation server (old UUID) belongs to guild: {tot_guild.get('name', tot_guild.get('_id'))}")
            else:
                logger.info("Tower of Temptation server not found in any guild")
        
        logger.info("Database check completed")
    except Exception as e:
        logger.error(f"Error checking database: {e}")
        import traceback
        logger.error(traceback.format_exc())
        
if __name__ == "__main__":
    asyncio.run(main())