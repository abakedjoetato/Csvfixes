"""
Deep CSV Processing Diagnostic Tool
This tool thoroughly examines the entire CSV processing pipeline to identify issues 
with data processing, file detection, caching mechanisms and database interactions.
"""

import asyncio
import os
import re
import csv
import io
import logging
import sys
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Set, Tuple

try:
    import motor.motor_asyncio
    MONGODB_AVAILABLE = True
except ImportError:
    MONGODB_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('csv_deep_diagnostic.log')
    ]
)

logger = logging.getLogger(__name__)

class DiagnosticStats:
    """Collect diagnostic statistics"""
    def __init__(self):
        self.csv_files_found = 0
        self.csv_files_processed = 0
        self.events_extracted = 0
        self.unique_players = set()
        self.unique_servers = set()
        self.unique_weapons = set()
        self.errors = []
        self.warnings = []
        self.duplicate_files = 0
        self.empty_files = 0
        self.processing_errors = 0
        self.start_time = time.time()
        
    def add_error(self, error):
        """Add an error to the stats"""
        self.errors.append(error)
        logger.error(error)
        
    def add_warning(self, warning):
        """Add a warning to the stats"""
        self.warnings.append(warning)
        logger.warning(warning)
        
    def summary(self):
        """Generate a summary of the diagnostic stats"""
        elapsed = time.time() - self.start_time
        summary = (
            f"\n{'='*60}\n"
            f"CSV PROCESSING DIAGNOSTIC SUMMARY\n"
            f"{'='*60}\n"
            f"CSV Files Found: {self.csv_files_found}\n"
            f"CSV Files Processed: {self.csv_files_processed}\n"
            f"Empty Files: {self.empty_files}\n"
            f"Duplicate Files: {self.duplicate_files}\n"
            f"Processing Errors: {self.processing_errors}\n"
            f"Events Extracted: {self.events_extracted}\n"
            f"Unique Players Identified: {len(self.unique_players)}\n"
            f"Unique Servers Identified: {len(self.unique_servers)}\n"
            f"Unique Weapons Detected: {len(self.unique_weapons)}\n"
            f"Errors Encountered: {len(self.errors)}\n"
            f"Warnings Issued: {len(self.warnings)}\n"
            f"Elapsed Time: {elapsed:.2f} seconds\n"
            f"{'='*60}\n"
        )
        
        if self.errors:
            summary += "\nERRORS:\n"
            for i, error in enumerate(self.errors[:10], 1):
                summary += f"{i}. {error}\n"
            if len(self.errors) > 10:
                summary += f"...and {len(self.errors) - 10} more errors\n"
        
        if self.warnings:
            summary += "\nWARNINGS:\n"
            for i, warning in enumerate(self.warnings[:10], 1):
                summary += f"{i}. {warning}\n"
            if len(self.warnings) > 10:
                summary += f"...and {len(self.warnings) - 10} more warnings\n"
                
        return summary

# Global statistics
stats = DiagnosticStats()

async def get_database():
    """Get MongoDB database connection"""
    if not MONGODB_AVAILABLE:
        stats.add_warning("MongoDB driver not available, database checks skipped")
        return None
        
    try:
        # MongoDB connection info
        mongo_uri = os.environ.get("MONGODB_URI", "mongodb://localhost:27017")
        db_name = os.environ.get("MONGODB_DB", "emeralds_killfeed")
        
        # Create client and get database
        client = motor.motor_asyncio.AsyncIOMotorClient(mongo_uri)
        db = client[db_name]
        
        logger.info(f"Successfully connected to MongoDB database: {db_name}")
        return db
    except Exception as e:
        stats.add_error(f"Failed to connect to MongoDB: {str(e)}")
        return None

async def check_database_connectivity():
    """Verify database connectivity and examine collections"""
    logger.info("Checking database connectivity and collections...")
    
    db = await get_database()
    if not db:
        return
    
    try:
        # Get list of collections
        collections = await db.list_collection_names()
        logger.info(f"Found {len(collections)} collections: {collections}")
        
        # Check critical collections
        critical_collections = ["servers", "game_servers", "guilds", "kills", "players", "rivalries"]
        for collection in critical_collections:
            if collection in collections:
                count = await db[collection].count_documents({})
                logger.info(f"Collection '{collection}' has {count} documents")
            else:
                stats.add_warning(f"Critical collection '{collection}' not found in database")
    except Exception as e:
        stats.add_error(f"Error examining database: {str(e)}")

async def examine_server_configs():
    """Examine server configurations in the database"""
    logger.info("Examining server configurations...")
    
    db = await get_database()
    if not db:
        return
    
    server_configs = {}
    
    try:
        # Check standalone servers collection
        if await db.command("collstats", "servers"):
            async for server in db.servers.find({}):
                server_id = server.get("_id", "unknown")
                stats.unique_servers.add(server_id)
                logger.info(f"Found server in servers collection: {server_id}")
                
                # Check SFTP configuration
                sftp_config = {
                    "hostname": server.get("sftp_host"),
                    "port": server.get("sftp_port"),
                    "username": server.get("sftp_username"),
                    "password": server.get("sftp_password"),
                    "path": server.get("sftp_path")
                }
                
                if all(sftp_config.values()):
                    logger.info(f"Server {server_id} has complete SFTP configuration")
                    server_configs[server_id] = sftp_config
                else:
                    missing = [k for k, v in sftp_config.items() if not v]
                    stats.add_warning(f"Server {server_id} missing SFTP configuration: {missing}")
    except Exception as e:
        stats.add_warning(f"Error examining servers collection: {str(e)}")
    
    try:
        # Check game_servers collection
        if await db.command("collstats", "game_servers"):
            async for server in db.game_servers.find({}):
                server_id = server.get("_id", "unknown")
                stats.unique_servers.add(server_id)
                logger.info(f"Found server in game_servers collection: {server_id}")
                
                # Check SFTP configuration
                sftp_config = {
                    "hostname": server.get("hostname"),
                    "port": server.get("port"),
                    "username": server.get("username"),
                    "password": server.get("password"),
                    "path": server.get("path")
                }
                
                if all(sftp_config.values()):
                    logger.info(f"Game server {server_id} has complete SFTP configuration")
                    server_configs[server_id] = sftp_config
                else:
                    missing = [k for k, v in sftp_config.items() if not v]
                    stats.add_warning(f"Game server {server_id} missing SFTP configuration: {missing}")
    except Exception as e:
        stats.add_warning(f"Error examining game_servers collection: {str(e)}")
    
    try:
        # Check guild-embedded server configurations
        if await db.command("collstats", "guilds"):
            async for guild in db.guilds.find({}):
                guild_id = guild.get("_id", "unknown")
                servers = guild.get("servers", [])
                
                for server in servers:
                    server_id = server.get("server_id", "unknown")
                    stats.unique_servers.add(server_id)
                    logger.info(f"Found server in guild {guild_id}: {server_id}")
                    
                    # Check SFTP configuration
                    sftp_config = {
                        "hostname": server.get("hostname"),
                        "port": server.get("port"),
                        "username": server.get("username"),
                        "password": server.get("password"),
                        "path": server.get("path")
                    }
                    
                    if all(sftp_config.values()):
                        logger.info(f"Guild server {server_id} has complete SFTP configuration")
                        server_configs[server_id] = sftp_config
                    else:
                        missing = [k for k, v in sftp_config.items() if not v]
                        stats.add_warning(f"Guild server {server_id} missing SFTP configuration: {missing}")
    except Exception as e:
        stats.add_warning(f"Error examining guild servers: {str(e)}")
    
    logger.info(f"Found {len(server_configs)} servers with complete SFTP configuration")
    return server_configs

async def analyze_csv_files(server_configs):
    """Analyze CSV files accessible to the system"""
    logger.info("Analyzing CSV files...")
    
    # First check local CSV files in attached_assets
    local_csv_files = []
    if os.path.exists("attached_assets"):
        local_csv_files = [
            os.path.join("attached_assets", f) 
            for f in os.listdir("attached_assets") 
            if f.endswith(".csv")
        ]
        
        stats.csv_files_found += len(local_csv_files)
        logger.info(f"Found {len(local_csv_files)} local CSV files in attached_assets")
        
        # Sample the local files
        for file_path in local_csv_files:
            if not os.path.getsize(file_path):
                stats.empty_files += 1
                stats.add_warning(f"Empty CSV file: {file_path}")
                continue
                
            try:
                with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                    content = f.read()
                
                if not content.strip():
                    stats.empty_files += 1
                    stats.add_warning(f"CSV file with only whitespace: {file_path}")
                    continue
                
                # Detect delimiter
                semicolon_count = content.count(';')
                comma_count = content.count(',')
                detected_delimiter = ';' if semicolon_count >= comma_count else ','
                
                # Try to parse and extract events
                content_io = io.StringIO(content)
                reader = csv.reader(content_io, delimiter=detected_delimiter)
                
                events = []
                for row in reader:
                    if len(row) < 6:  # Skip rows with insufficient fields
                        continue
                    
                    # Extract timestamp
                    timestamp_str = row[0].strip() if row[0] else None
                    if not timestamp_str:
                        continue
                        
                    # Try to parse timestamp
                    timestamp = None
                    for fmt in ["%Y.%m.%d-%H.%M.%S", "%Y-%m-%d %H:%M:%S", "%Y.%m.%d %H:%M:%S"]:
                        try:
                            timestamp = datetime.strptime(timestamp_str, fmt)
                            break
                        except ValueError:
                            continue
                    
                    if not timestamp:
                        logger.warning(f"Could not parse timestamp: {timestamp_str}")
                        continue
                    
                    # Extract other fields
                    killer_name = row[1].strip() if len(row) > 1 else "Unknown"
                    killer_id = row[2].strip() if len(row) > 2 else "Unknown"
                    victim_name = row[3].strip() if len(row) > 3 else "Unknown"
                    victim_id = row[4].strip() if len(row) > 4 else "Unknown"
                    weapon = row[5].strip() if len(row) > 5 else "Unknown"
                    
                    # Add to event list
                    events.append({
                        "timestamp": timestamp,
                        "killer_name": killer_name,
                        "killer_id": killer_id,
                        "victim_name": victim_name,
                        "victim_id": victim_id,
                        "weapon": weapon
                    })
                    
                    # Update stats
                    stats.unique_players.add(killer_id)
                    stats.unique_players.add(victim_id)
                    stats.unique_weapons.add(weapon)
                
                logger.info(f"Extracted {len(events)} events from {file_path}")
                stats.events_extracted += len(events)
                stats.csv_files_processed += 1
                
            except Exception as e:
                stats.processing_errors += 1
                stats.add_error(f"Error processing {file_path}: {str(e)}")
    else:
        stats.add_warning("attached_assets directory not found")
    
    # Show analysis results
    if stats.csv_files_processed > 0:
        logger.info(f"Successfully processed {stats.csv_files_processed} CSV files")
        logger.info(f"Extracted a total of {stats.events_extracted} events")
        logger.info(f"Found {len(stats.unique_players)} unique players")
        logger.info(f"Found {len(stats.unique_weapons)} unique weapons")
    else:
        stats.add_warning("No CSV files were successfully processed")

async def check_kill_documents():
    """Check if kill documents are being stored properly"""
    logger.info("Checking kill documents in database...")
    
    db = await get_database()
    if not db:
        return
    
    try:
        # Check if kills collection exists and has documents
        kills_count = await db.kills.count_documents({})
        logger.info(f"Found {kills_count} documents in kills collection")
        
        if kills_count == 0:
            stats.add_warning("No kill documents found in database")
            return
        
        # Get a sample of recent kills for analysis
        recent_kills = []
        async for kill in db.kills.find().sort([("timestamp", -1)]).limit(10):
            recent_kills.append(kill)
        
        if not recent_kills:
            stats.add_warning("No recent kill documents found")
            return
        
        # Analyze the most recent kill
        latest_kill = recent_kills[0]
        logger.info(f"Most recent kill document: {latest_kill}")
        
        # Check timestamp format
        timestamp = latest_kill.get("timestamp")
        if timestamp:
            if isinstance(timestamp, datetime):
                logger.info(f"Kill timestamp is properly stored as datetime: {timestamp}")
            else:
                stats.add_warning(f"Kill timestamp is not stored as datetime: {type(timestamp)}")
        else:
            stats.add_warning("Kill document missing timestamp field")
        
        # Check critical fields
        critical_fields = ["killer_id", "victim_id", "weapon", "server_id"]
        for field in critical_fields:
            if field not in latest_kill:
                stats.add_warning(f"Kill document missing critical field: {field}")
        
        # Check for most recent kill
        now = datetime.now()
        one_day_ago = now - timedelta(days=1)
        recent_count = await db.kills.count_documents({"timestamp": {"$gt": one_day_ago}})
        
        logger.info(f"Found {recent_count} kills in the last 24 hours")
        if recent_count == 0:
            stats.add_warning("No kill documents from the last 24 hours")
        
    except Exception as e:
        stats.add_error(f"Error checking kill documents: {str(e)}")

async def analyze_cached_data():
    """Analyze in-memory caches that might be affecting processing"""
    logger.info("Analyzing possible cached data...")
    
    # Check for cache files
    cache_files = [
        f for f in os.listdir('.') 
        if f.endswith('.cache') or 'cache' in f.lower() or f.endswith('.tmp')
    ]
    
    if cache_files:
        logger.info(f"Found {len(cache_files)} potential cache files: {cache_files}")
        for file in cache_files:
            try:
                with open(file, 'r') as f:
                    content = f.read()
                if 'csv' in content.lower():
                    logger.info(f"Cache file {file} contains references to CSV data")
            except:
                pass
    else:
        logger.info("No cache files found")
    
    # Check for CSV-related processing in logs
    log_files = [f for f in os.listdir('.') if f.endswith('.log')]
    for log_file in log_files:
        try:
            with open(log_file, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
            
            # Look for CSV processing mentions
            csv_mentions = content.count('CSV') + content.count('csv')
            if csv_mentions > 0:
                logger.info(f"Found {csv_mentions} mentions of CSV in log file {log_file}")
                
                # Look for errors related to CSV
                error_lines = [line for line in content.splitlines() if 'ERROR' in line and 'CSV' in line]
                if error_lines:
                    logger.info(f"Found {len(error_lines)} CSV-related error lines in {log_file}")
                    for line in error_lines[:5]:
                        logger.info(f"Error from logs: {line}")
        except Exception as e:
            logger.warning(f"Could not analyze log file {log_file}: {str(e)}")

async def get_memory_usage():
    """Get current process memory usage"""
    try:
        import psutil
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        memory_mb = memory_info.rss / (1024 * 1024)
        logger.info(f"Current memory usage: {memory_mb:.2f} MB")
    except ImportError:
        logger.info("psutil not available, cannot get memory usage")
    except Exception as e:
        logger.warning(f"Error getting memory usage: {str(e)}")

async def main():
    """Main diagnostic function"""
    logger.info("Starting deep CSV processing diagnostic")
    
    try:
        # Memory usage at start
        await get_memory_usage()
        
        # Check database connectivity
        await check_database_connectivity()
        
        # Examine server configurations
        server_configs = await examine_server_configs()
        
        # Analyze CSV files
        await analyze_csv_files(server_configs)
        
        # Check kill documents
        await check_kill_documents()
        
        # Analyze cached data
        await analyze_cached_data()
        
        # Memory usage at end
        await get_memory_usage()
        
        # Generate summary
        summary = stats.summary()
        logger.info(summary)
        
        # Save summary to file
        with open('csv_diagnostic_summary.txt', 'w') as f:
            f.write(summary)
        
        logger.info("Deep CSV processing diagnostic completed")
        
    except Exception as e:
        logger.error(f"Unhandled error in diagnostic: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())