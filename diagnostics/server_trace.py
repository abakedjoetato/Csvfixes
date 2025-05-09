"""
Diagnostic tool to trace server configuration through the system
This script will help diagnose issues with server configuration and CSV file discovery
"""
import os
import sys
import json
import logging
import asyncio
import re
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple

# Ensure we can import from parent directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import MongoDB connection from config
from config import MONGODB_URI
import motor.motor_asyncio

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("diagnostics/server_trace.log", mode="w")
    ]
)
logger = logging.getLogger("server_trace")

async def connect_to_mongodb():
    """Connect to MongoDB using URI from environment variables"""
    client = motor.motor_asyncio.AsyncIOMotorClient(MONGODB_URI)
    db = client.towerdb  # Use the default database
    return db

async def trace_server_config(db, server_id: str, guild_id: Optional[str] = None):
    """Trace server configuration through all collections

    Args:
        db: Database connection
        server_id: Server ID to trace
        guild_id: Optional Guild ID for additional filtering
    """
    logger.info(f"=== TRACING SERVER CONFIG: server_id={server_id}, guild_id={guild_id} ===")
    
    # Standardize server_id
    std_server_id = standardize_server_id(server_id)
    logger.info(f"Standardized server_id: {std_server_id}")
    
    # Check guilds collection
    logger.info("== CHECKING GUILDS COLLECTION ==")
    guild_query = {}
    if guild_id:
        guild_query["guild_id"] = str(guild_id)
    guilds_cursor = db.guilds.find(guild_query)
    
    found_in_guilds = False
    async for guild in guilds_cursor:
        guild_id_val = guild.get("guild_id")
        servers = guild.get("servers", [])
        
        logger.info(f"Guild {guild_id_val}: Found {len(servers)} servers in guild.servers array")
        
        for i, server in enumerate(servers):
            if not isinstance(server, dict):
                logger.warning(f"Server at index {i} is not a dictionary: {type(server)}")
                continue
                
            server_id_val = server.get("server_id")
            std_val = standardize_server_id(server_id_val)
            server_name = server.get("server_name", "Unknown")
            
            if std_val == std_server_id:
                found_in_guilds = True
                logger.info(f"FOUND SERVER in guild {guild_id_val}!")
                logger.info(f"Server details: id={server_id_val}, name={server_name}")
                
                # Check for required SFTP fields
                has_sftp_host = "sftp_host" in server and server["sftp_host"]
                has_sftp_username = "sftp_username" in server and server["sftp_username"]
                has_sftp_password = "sftp_password" in server and server["sftp_password"]
                has_sftp_enabled = "sftp_enabled" in server and server["sftp_enabled"]
                
                logger.info(f"SFTP Fields: host={has_sftp_host}, username={has_sftp_username}, password={has_sftp_password}, enabled={has_sftp_enabled}")
                
                # Log all keys in server object
                logger.info(f"All server fields: {', '.join(server.keys())}")
                
                # Log critical values
                logger.info(f"sftp_host: {server.get('sftp_host')}")
                logger.info(f"sftp_path: {server.get('sftp_path', '/logs')}")
                break
                
    if not found_in_guilds:
        logger.warning(f"Server {std_server_id} NOT FOUND in any guild's servers array")
    
    # Check servers collection (used by CSV processor)
    logger.info("== CHECKING SERVERS COLLECTION ==")
    server_in_servers = await db.servers.find_one({"server_id": std_server_id})
    if server_in_servers:
        logger.info(f"FOUND SERVER in servers collection!")
        
        # Check for required SFTP fields
        has_sftp_host = "sftp_host" in server_in_servers and server_in_servers["sftp_host"]
        has_sftp_username = "sftp_username" in server_in_servers and server_in_servers["sftp_username"]
        has_sftp_password = "sftp_password" in server_in_servers and server_in_servers["sftp_password"]
        has_sftp_enabled = "sftp_enabled" in server_in_servers and server_in_servers["sftp_enabled"]
        
        logger.info(f"SFTP Fields: host={has_sftp_host}, username={has_sftp_username}, password={has_sftp_password}, enabled={has_sftp_enabled}")
        
        # Log all keys in server object
        logger.info(f"All server fields: {', '.join(server_in_servers.keys())}")
        
        # Log critical values
        logger.info(f"sftp_host: {server_in_servers.get('sftp_host')}")
        logger.info(f"sftp_path: {server_in_servers.get('sftp_path', '/logs')}")
    else:
        logger.warning(f"Server {std_server_id} NOT FOUND in servers collection")
    
    # Check game_servers collection
    logger.info("== CHECKING GAME_SERVERS COLLECTION ==")
    server_in_game_servers = await db.game_servers.find_one({"server_id": std_server_id})
    if server_in_game_servers:
        logger.info(f"FOUND SERVER in game_servers collection!")
        
        # Check for required SFTP fields
        has_sftp_host = "sftp_host" in server_in_game_servers and server_in_game_servers["sftp_host"]
        has_sftp_username = "sftp_username" in server_in_game_servers and server_in_game_servers["sftp_username"]
        has_sftp_password = "sftp_password" in server_in_game_servers and server_in_game_servers["sftp_password"]
        has_sftp_enabled = "sftp_enabled" in server_in_game_servers and server_in_game_servers["sftp_enabled"]
        
        logger.info(f"SFTP Fields: host={has_sftp_host}, username={has_sftp_username}, password={has_sftp_password}, enabled={has_sftp_enabled}")
        
        # Log all keys in server object
        logger.info(f"All server fields: {', '.join(server_in_game_servers.keys())}")
        
        # Log critical values
        logger.info(f"sftp_host: {server_in_game_servers.get('sftp_host')}")
        logger.info(f"sftp_path: {server_in_game_servers.get('sftp_path', '/logs')}")
    else:
        logger.warning(f"Server {std_server_id} NOT FOUND in game_servers collection")

def standardize_server_id(server_id):
    """Standardize server ID format
    Simplified version for diagnostic purposes
    """
    if server_id is None:
        return None
        
    # Convert to string
    str_id = str(server_id)
    
    # Handle quotes
    if (str_id.startswith('"') and str_id.endswith('"')) or \
       (str_id.startswith("'") and str_id.endswith("'")):
        str_id = str_id[1:-1].strip()
        
    # Handle cases where server ID includes quotes or other punctuation
    if not str_id.isdigit() and any(c in str_id for c in '"\'`.,;:'):
        for c in '"\'`.,;:':
            str_id = str_id.replace(c, '')
        str_id = str_id.strip()
        
    # Handle directory-style server IDs that might come from SFTP paths
    if '/' in str_id:
        # Take the last part of the path as it's likely the actual server ID
        path_parts = str_id.split('/')
        potential_id = path_parts[-1]
        str_id = potential_id.strip()
        
    # Check for hostname_serverid pattern
    if '_' in str_id and not str_id.isdigit():
        # If it has a hostname_serverid format, take the part after the last underscore
        parts = str_id.split('_')
        if len(parts) >= 2:
            # Check if the last part looks like a server ID
            if parts[-1].isdigit() or re.match(r'^[a-zA-Z0-9]+$', parts[-1]):
                str_id = parts[-1]
                
    return str_id

async def main():
    """Main function"""
    if len(sys.argv) < 2:
        print("Usage: python diagnostics/server_trace.py <server_id> [guild_id]")
        return
    
    server_id = sys.argv[1]
    guild_id = sys.argv[2] if len(sys.argv) > 2 else None
    
    db = await connect_to_mongodb()
    await trace_server_config(db, server_id, guild_id)

if __name__ == "__main__":
    asyncio.run(main())