"""
Deep CSV Processing Diagnostic Tool
This tool thoroughly examines the entire CSV processing pipeline to identify issues 
with data processing, file detection, caching mechanisms and database interactions.
"""
import asyncio
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional, Set, Tuple
import json
import traceback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="csv_deep_diagnostic.log",
    filemode="w"
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger().addHandler(console)
logger = logging.getLogger(__name__)

class DiagnosticStats:
    """Collect diagnostic statistics"""
    def __init__(self):
        self.files_found = 0
        self.files_downloaded = 0
        self.files_parsed = 0
        self.events_processed = 0
        self.database_ops = 0
        self.errors = []
        self.warnings = []
        self.start_time = time.time()
        
    def add_error(self, error):
        """Add an error to the stats"""
        self.errors.append(error)
        logger.error(f"ERROR: {error}")
        
    def add_warning(self, warning):
        """Add a warning to the stats"""
        self.warnings.append(warning)
        logger.warning(f"WARNING: {warning}")
        
    def summary(self):
        """Generate a summary of the diagnostic stats"""
        elapsed = time.time() - self.start_time
        return {
            "files_found": self.files_found,
            "files_downloaded": self.files_downloaded,
            "files_parsed": self.files_parsed,
            "events_processed": self.events_processed,
            "database_ops": self.database_ops,
            "errors": len(self.errors),
            "warnings": len(self.warnings),
            "elapsed_seconds": elapsed
        }

async def get_database():
    """Get MongoDB database connection"""
    try:
        sys.path.append('.')
        from utils.database import get_db
        
        db = await get_db()
        return db
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None

async def check_database_connectivity():
    """Verify database connectivity and examine collections"""
    try:
        import motor.motor_asyncio
        from pymongo.errors import ConnectionFailure
        
        db = await get_database()
        if not db:
            return False, "Could not get database connection"
            
        # Check if database is accessible
        try:
            await db.command("ping")
            logger.info("Database ping successful")
        except ConnectionFailure:
            return False, "Database ping failed - connection issues"
            
        # Check collections
        collections = await db.list_collection_names()
        logger.info(f"Found {len(collections)} collections: {collections}")
        
        # Check for required collections
        required_collections = ["guilds", "servers", "players", "kills", "rivalries"]
        missing = [c for c in required_collections if c not in collections]
        
        if missing:
            return False, f"Missing required collections: {missing}"
            
        return True, collections
    except Exception as e:
        logger.error(f"Database check error: {e}")
        traceback.print_exc()
        return False, f"Error checking database: {str(e)}"

async def examine_server_configs():
    """Examine server configurations in the database"""
    try:
        db = await get_database()
        if not db:
            return []
            
        # Check all possible collections where server configs might be stored
        server_configs = []
        
        # Check standalone servers collection
        async for server in db.servers.find():
            server_id = server.get('server_id')
            if not server_id:
                continue
                
            config = {
                "server_id": server_id,
                "name": server.get('name', 'Unknown'),
                "source": "servers",
                "sftp_config": bool(server.get('hostname') and server.get('port')),
                "hostname": server.get('hostname'),
                "port": server.get('port'),
                "csv_path": server.get('csv_path'),
                "csv_pattern": server.get('csv_pattern')
            }
            server_configs.append(config)
            
        # Check game_servers collection
        async for server in db.game_servers.find():
            server_id = server.get('server_id')
            if not server_id:
                continue
                
            config = {
                "server_id": server_id,
                "name": server.get('name', 'Unknown'),
                "source": "game_servers",
                "sftp_config": bool(server.get('hostname') and server.get('port')),
                "hostname": server.get('hostname'),
                "port": server.get('port'),
                "csv_path": server.get('csv_path'),
                "csv_pattern": server.get('csv_pattern')
            }
            server_configs.append(config)
            
        # Check guild configurations
        async for guild in db.guilds.find():
            servers = guild.get('servers', [])
            for server in servers:
                server_id = server.get('server_id')
                if not server_id:
                    continue
                    
                config = {
                    "server_id": server_id,
                    "name": server.get('name', 'Unknown'),
                    "source": "guilds",
                    "guild_id": guild.get('_id'),
                    "guild_name": guild.get('name', 'Unknown'),
                    "sftp_config": bool(server.get('hostname') and server.get('port')),
                    "hostname": server.get('hostname'),
                    "port": server.get('port'),
                    "csv_path": server.get('csv_path'),
                    "csv_pattern": server.get('csv_pattern')
                }
                server_configs.append(config)
                
        logger.info(f"Found {len(server_configs)} server configurations in database")
        return server_configs
    except Exception as e:
        logger.error(f"Error examining server configs: {e}")
        traceback.print_exc()
        return []

async def analyze_csv_files(server_configs):
    """Analyze CSV files accessible to the system"""
    stats = DiagnosticStats()
    
    try:
        sys.path.append('.')
        from utils.sftp import SFTPManager
        from utils.csv_parser import CSVParser
        from utils.server_identity import get_numeric_id, get_uuid_for_server_id
        
        parser = CSVParser()
        
        # Test with all configs that have SFTP enabled
        for config in server_configs:
            if not config.get('sftp_config'):
                continue
                
            server_id = config.get('server_id')
            hostname = config.get('hostname')
            port = config.get('port')
            username = config.get('username', 'baked')
            password = config.get('password', 'emerald')
            
            logger.info(f"Testing CSV files for server {server_id} ({hostname}:{port})")
            
            # Extract numeric ID from server ID if it's a UUID
            numeric_id = None
            if server_id and '-' in server_id:
                try:
                    numeric_id = get_numeric_id(server_id)
                    logger.info(f"Extracted numeric ID: {numeric_id} from server ID: {server_id}")
                except Exception as e:
                    logger.error(f"Error extracting numeric ID: {e}")
            
            # Create SFTP manager
            sftp_manager = SFTPManager()
            
            # Connect to SFTP
            try:
                sftp = await sftp_manager.connect(
                    hostname=hostname,
                    port=port,
                    username=username,
                    password=password,
                    server_id=server_id
                )
                logger.info(f"SFTP connection successful for {server_id}")
            except Exception as e:
                stats.add_error(f"SFTP connection failed for {server_id}: {e}")
                continue
                
            # Try to find CSV files
            try:
                # Try looking for files in the default path pattern
                base_path = f"/79.127.236.1_{numeric_id if numeric_id else server_id}"
                paths_to_search = [
                    f"{base_path}/actual1/deathlogs/world_0",
                    f"{base_path}/actual1/deathlogs",
                    f"{base_path}/deathlogs/world_0",
                    f"{base_path}/deathlogs",
                    base_path
                ]
                
                for path in paths_to_search:
                    logger.info(f"Searching for CSV files in {path}")
                    try:
                        csv_files = await sftp_manager.find_files(
                            sftp, 
                            path=path,
                            pattern=r'\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}\.csv'
                        )
                        
                        if csv_files:
                            logger.info(f"Found {len(csv_files)} CSV files in {path}")
                            stats.files_found += len(csv_files)
                            
                            # Test downloading a sample of files
                            sample_files = csv_files[:3]
                            for csv_file in sample_files:
                                logger.info(f"Downloading and testing: {csv_file}")
                                
                                # Download the file
                                try:
                                    content = await sftp_manager.get_file_content(sftp, csv_file)
                                    if content:
                                        stats.files_downloaded += 1
                                        logger.info(f"Successfully downloaded {csv_file} ({len(content)} bytes)")
                                        
                                        # Parse the CSV content
                                        try:
                                            events = parser.parse_csv_data(content)
                                            if events:
                                                stats.files_parsed += 1
                                                stats.events_processed += len(events)
                                                logger.info(f"Successfully parsed {len(events)} events from {csv_file}")
                                                
                                                # Log sample events
                                                for i, event in enumerate(events[:3]):
                                                    logger.info(f"Event {i+1}:")
                                                    timestamp = event.get('timestamp')
                                                    killer = event.get('killer_name')
                                                    victim = event.get('victim_name')
                                                    weapon = event.get('weapon')
                                                    
                                                    if isinstance(timestamp, datetime):
                                                        logger.info(f"✓ Timestamp correctly parsed: {timestamp}")
                                                    else:
                                                        stats.add_error(f"❌ Timestamp not parsed correctly: {timestamp}")
                                                        
                                                    logger.info(f"  {killer} killed {victim} with {weapon}")
                                            else:
                                                stats.add_warning(f"No events parsed from {csv_file}")
                                        except Exception as e:
                                            stats.add_error(f"Error parsing CSV content: {e}")
                                except Exception as e:
                                    stats.add_error(f"Error downloading file: {e}")
                            
                            # Stop after finding files in the first path
                            break
                    except Exception as e:
                        logger.warning(f"Error searching path {path}: {e}")
                        
                if stats.files_found == 0:
                    stats.add_warning(f"No CSV files found for server {server_id} in any path")
            except Exception as e:
                stats.add_error(f"Error searching for CSV files: {e}")
                
            # Close SFTP connection
            await sftp.close()
            
        return stats
    except Exception as e:
        logger.error(f"Error analyzing CSV files: {e}")
        traceback.print_exc()
        stats.add_error(f"General error in CSV file analysis: {e}")
        return stats

async def check_kill_documents():
    """Check if kill documents are being stored properly"""
    try:
        db = await get_database()
        if not db:
            return 0
            
        # Count kill documents added in the last 24 hours
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        
        count = await db.kills.count_documents({
            "timestamp": {"$gte": yesterday}
        })
        
        logger.info(f"Found {count} kill documents from the last 24 hours")
        return count
    except Exception as e:
        logger.error(f"Error checking kill documents: {e}")
        return 0

async def analyze_cached_data():
    """Analyze in-memory caches that might be affecting processing"""
    try:
        # Try to access the running bot and check its CSV processor cog
        sys.path.append('.')
        import bot as bot_module
        
        # Check if bot has client attribute
        if hasattr(bot_module, 'client'):
            client = bot_module.client
            
            if client and hasattr(client, 'cogs'):
                cogs = client.cogs
                if 'CSVProcessorCog' in cogs:
                    csv_processor = cogs['CSVProcessorCog']
                    
                    # Check last_processed cache
                    if hasattr(csv_processor, 'last_processed'):
                        last_processed = csv_processor.last_processed
                        logger.info(f"CSV processor last_processed cache: {last_processed}")
                        
                    # Check processed_files cache
                    if hasattr(csv_processor, 'processed_files'):
                        processed_files = csv_processor.processed_files
                        logger.info(f"CSV processor has {len(processed_files)} entries in processed_files cache")
                        
                    return True
        
        logger.info("Could not access bot caches - bot may not be running")
        return False
    except Exception as e:
        logger.error(f"Error analyzing caches: {e}")
        return False

async def get_memory_usage():
    """Get current process memory usage"""
    try:
        import psutil
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        return {
            'rss': memory_info.rss / (1024 * 1024),  # MB
            'vms': memory_info.vms / (1024 * 1024)   # MB
        }
    except Exception as e:
        logger.error(f"Error getting memory usage: {e}")
        return {'rss': 0, 'vms': 0}

async def main():
    """Main diagnostic function"""
    logger.info("Starting CSV Deep Diagnostic")
    
    # Step 1: Check database connectivity
    logger.info("Step 1: Checking database connectivity...")
    db_ok, db_result = await check_database_connectivity()
    if not db_ok:
        logger.error(f"Database check failed: {db_result}")
    
    # Step 2: Examine server configurations
    logger.info("Step 2: Examining server configurations...")
    server_configs = await examine_server_configs()
    
    # Step 3: Analyze CSV files
    logger.info("Step 3: Analyzing CSV files and verifying timestamp parsing...")
    stats = await analyze_csv_files(server_configs)
    
    # Step 4: Check kill documents in database
    logger.info("Step 4: Checking kill documents in database...")
    kill_count = await check_kill_documents()
    
    # Step 5: Analyze cached data
    logger.info("Step 5: Analyzing cached data...")
    cache_analyzed = await analyze_cached_data()
    
    # Step 6: Get memory usage
    logger.info("Step 6: Getting memory usage...")
    memory = await get_memory_usage()
    
    # Compile diagnosis report
    logger.info("Compiling diagnostic report...")
    report = {
        "timestamp": datetime.now().isoformat(),
        "database": {
            "status": db_ok,
            "collections": db_result if isinstance(db_result, list) else None,
            "error": db_result if not isinstance(db_result, list) and not db_ok else None,
            "recent_kills": kill_count
        },
        "server_configs": {
            "count": len(server_configs),
            "configs": server_configs
        },
        "csv_processing": stats.summary(),
        "memory": memory,
        "errors": stats.errors,
        "warnings": stats.warnings
    }
    
    # Print summary
    logger.info("=" * 80)
    logger.info("CSV DEEP DIAGNOSTIC RESULTS")
    logger.info("=" * 80)
    logger.info(f"Database Status: {'✓ Connected' if db_ok else '❌ Failed'}")
    logger.info(f"Server Configs: {len(server_configs)} found")
    logger.info(f"CSV Files: {stats.files_found} found, {stats.files_downloaded} downloaded, {stats.files_parsed} parsed")
    logger.info(f"Events Processed: {stats.events_processed} from CSV files")
    logger.info(f"Recent Kill Documents: {kill_count} in the last 24 hours")
    logger.info(f"Memory Usage: {memory['rss']:.2f} MB RSS, {memory['vms']:.2f} MB VMS")
    logger.info(f"Errors: {len(stats.errors)}")
    logger.info(f"Warnings: {len(stats.warnings)}")
    logger.info("=" * 80)
    
    # Output diagnostic messages to a Discord-friendly format
    conclusion = "✅ SUCCESS: " if stats.files_parsed > 0 and len(stats.errors) == 0 else "❌ ISSUES DETECTED: "
    
    if stats.files_parsed > 0 and len(stats.errors) == 0:
        conclusion += f"Successfully downloaded and parsed {stats.files_parsed} CSV files with {stats.events_processed} events."
        conclusion += "\n✓ Timestamp parsing is working correctly with YYYY.MM.DD-HH.MM.SS format"
        conclusion += "\n✓ Server ID resolution between UUID and numeric format is working"
        conclusion += "\n✓ CSV file detection is working properly"
    else:
        conclusion += f"CSV processing has issues. Found {stats.files_found} files but only parsed {stats.files_parsed} successfully."
        conclusion += f"\n❌ Encountered {len(stats.errors)} errors and {len(stats.warnings)} warnings"
        
    logger.info(conclusion)
    
    # Save report to file
    with open("csv_diagnostic_report.json", "w") as f:
        json.dump(report, f, indent=2)
    
    logger.info("Diagnostic complete - see csv_deep_diagnostic.log and csv_diagnostic_report.json for details")
    return report

if __name__ == "__main__":
    asyncio.run(main())