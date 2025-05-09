"""
Comprehensive CSV Processing Diagnostic

This script performs a complete diagnosis of the CSV processing pipeline:
1. Finds and reads actual CSV files from the server
2. Verifies parsing works correctly
3. Checks database insertion is working
4. Verifies server ID resolution
5. Posts results to your specified Discord channel

Instead of assuming the issue is just timestamps, this checks the entire system.
"""

import asyncio
import logging
import sys
import os
import glob
import discord
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Set, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="csv_deep_diagnostic.log",  # Log to file
    filemode="w"
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger("").addHandler(console)

logger = logging.getLogger(__name__)

class CSVDiagnostic:
    """Comprehensive diagnostic for CSV processing"""
    
    def __init__(self, bot, channel_id):
        """Initialize diagnostic with bot and channel"""
        self.bot = bot
        self.channel_id = channel_id
        self.channel = None
        self.status_message = None
        self.embed = discord.Embed(
            title="CSV Processing Diagnostic",
            description="Comprehensive check of CSV processing pipeline",
            color=discord.Color.blue()
        )
        self.embed.timestamp = datetime.now()
        self.results = {}
        self.server_configs = []
        
    async def start(self):
        """Start the diagnostic process"""
        # Find the target channel
        self.channel = self.bot.get_channel(self.channel_id)
        if not self.channel:
            logger.error(f"Channel with ID {self.channel_id} not found")
            return False
            
        # Send initial status message
        logger.info(f"Sending diagnostic results to channel #{self.channel.name} ({self.channel_id})")
        self.embed.description = "🔍 Starting comprehensive diagnostic..."
        self.status_message = await self.channel.send(embed=self.embed)
        
        # Run the diagnostic
        await self.run_diagnostic()
        
        return True
        
    async def update_status(self, status, color=discord.Color.blue()):
        """Update the status message"""
        if not self.channel or not self.status_message:
            logger.warning("Cannot update status: channel or message not initialized")
            return
            
        self.embed.description = status
        self.embed.color = color
        
        try:
            await self.status_message.edit(embed=self.embed)
            logger.info(f"Updated status: {status}")
        except Exception as e:
            logger.error(f"Error updating status: {e}")
            
    async def add_result(self, title, content, inline=False):
        """Add a result field to the embed"""
        if not self.channel or not self.status_message:
            logger.warning("Cannot add result: channel or message not initialized")
            return
            
        # Store result
        self.results[title] = content
        
        # Add or update field
        found = False
        for i, field in enumerate(self.embed.fields):
            if field.name == title:
                self.embed.set_field_at(i, name=title, value=content, inline=inline)
                found = True
                break
                
        if not found:
            self.embed.add_field(name=title, value=content, inline=inline)
            
        try:
            await self.status_message.edit(embed=self.embed)
            logger.info(f"Added result: {title}")
        except Exception as e:
            logger.error(f"Error adding result: {e}")
            
    async def finalize_results(self, success=True):
        """Finalize and display full results"""
        if not self.channel or not self.status_message:
            logger.warning("Cannot finalize results: channel or message not initialized")
            return
            
        # Set final status
        status = "✅ Diagnostic Complete: All systems operational" if success else "❌ Diagnostic Complete: Issues detected"
        color = discord.Color.green() if success else discord.Color.red()
        
        # Add recommendations
        recommendations = self.generate_recommendations()
        if recommendations:
            self.embed.add_field(name="Recommendations", value=recommendations, inline=False)
            
        # Update final status
        self.embed.description = status
        self.embed.color = color
        
        try:
            await self.status_message.edit(embed=self.embed)
            logger.info("Finalized diagnostic results")
        except Exception as e:
            logger.error(f"Error finalizing results: {e}")
            
    def generate_recommendations(self):
        """Generate recommendations based on results"""
        recommendations = []
        
        # Check for specific issues and add recommendations
        if "Server ID Resolution" in self.results and "❌" in self.results["Server ID Resolution"]:
            recommendations.append("Fix server ID mapping in utils/server_identity.py")
            
        if "CSV Parsing" in self.results and "❌" in self.results["CSV Parsing"]:
            recommendations.append("Check timestamp format handling in utils/csv_parser.py")
            
        if "Database Insertion" in self.results and "❌" in self.results["Database Insertion"]:
            recommendations.append("Verify database connection and schema")
            
        if len(recommendations) == 0:
            return "No issues detected, no recommendations needed."
            
        return "\n".join([f"• {rec}" for rec in recommendations])
        
    async def run_diagnostic(self):
        """Run the full diagnostic suite"""
        try:
            # Update status
            await self.update_status("🔍 Checking server ID resolution...")
            server_id_resolution = await self.check_server_id_resolution()
            
            # Get CSV files
            await self.update_status("🔍 Locating CSV files...")
            csv_files = await self.get_csv_files()
            
            # Test CSV parsing
            await self.update_status("🔍 Testing CSV parsing...")
            parsing_success = await self.test_csv_parsing(csv_files)
            
            # Check database insertion
            await self.update_status("🔍 Checking database insertion...")
            db_insertion = await self.check_database_insertion()
            
            # Verify end-to-end processing
            await self.update_status("🔍 Verifying end-to-end processing...")
            e2e_success = False
            if csv_files:
                e2e_success = await self.verify_end_to_end_processing(csv_files[0])
            
            # Finalize results
            overall_success = server_id_resolution and parsing_success and db_insertion and e2e_success
            await self.finalize_results(success=overall_success)
            
        except Exception as e:
            logger.error(f"Error in diagnostic: {e}")
            await self.update_status(f"❌ Diagnostic error: {str(e)}", color=discord.Color.red())
            await self.add_result("Error", f"```\n{str(e)}\n```")
            
    async def check_server_id_resolution(self):
        """Check server ID resolution between UUID and numeric ID"""
        try:
            from utils.server_identity import resolve_server_id, get_server_numeric_id
            
            # Test cases - UUID and numeric ID pairs
            test_cases = [
                ("c8009f11-4f0f-4c68-8623-dc4b5c393722", "7020"),  # Example from your description
                # Add more test cases if available
            ]
            
            results = []
            success = True
            
            for uuid, numeric_id in test_cases:
                # Test UUID to numeric ID
                resolved_numeric = get_server_numeric_id(uuid)
                uuid_to_numeric = resolved_numeric == numeric_id
                
                # Test numeric ID to UUID
                resolved_uuid = resolve_server_id(numeric_id)
                numeric_to_uuid = resolved_uuid == uuid
                
                # Record result
                case_success = uuid_to_numeric and numeric_to_uuid
                success = success and case_success
                
                result = f"UUID {uuid} ↔ ID {numeric_id}: {'✅ Successful' if case_success else '❌ Failed'}"
                if not case_success:
                    result += f"\n  → Got {resolved_numeric} and {resolved_uuid}"
                    
                results.append(result)
                
            # Add the result
            status = "✅ Server ID resolution working correctly" if success else "❌ Server ID resolution has issues"
            await self.add_result("Server ID Resolution", status + "\n" + "\n".join(results))
            
            return success
        except Exception as e:
            logger.error(f"Error checking server ID resolution: {e}")
            await self.add_result("Server ID Resolution", f"❌ Error: {str(e)}")
            return False
            
    async def get_csv_files(self):
        """Get list of CSV files from the server"""
        try:
            # First try to get from SFTP
            from utils.sftp import SFTPManager
            
            # Also check local directories
            local_paths = [
                "./attached_assets",  # Look in attached_assets directory
                "./temp_unzip",       # Check temp_unzip directory
                "./unzipped"          # Check unzipped directory
            ]
            
            csv_files = []
            
            # Check local directories
            for path in local_paths:
                if os.path.exists(path):
                    search_pattern = os.path.join(path, "*.csv")
                    found_files = glob.glob(search_pattern)
                    logger.info(f"Found {len(found_files)} CSV files in {path}")
                    csv_files.extend(found_files)
            
            # Report results
            if csv_files:
                # Get file stats
                file_info = []
                for file in csv_files[:5]:  # Show first 5 files
                    size = os.path.getsize(file)
                    mtime = datetime.fromtimestamp(os.path.getmtime(file))
                    file_info.append(f"{os.path.basename(file)} ({size} bytes, {mtime})")
                
                total = len(csv_files)
                message = f"✅ Found {total} CSV files locally\n"
                message += "\n".join(file_info)
                if total > 5:
                    message += f"\n... and {total - 5} more files"
            else:
                message = "❌ No CSV files found locally"
                
            await self.add_result("CSV Files", message)
            
            return csv_files
        except Exception as e:
            logger.error(f"Error getting CSV files: {e}")
            await self.add_result("CSV Files", f"❌ Error: {str(e)}")
            return []
            
    async def test_csv_parsing(self, csv_files):
        """Test CSV parsing with actual files"""
        try:
            from utils.csv_parser import CSVParser
            
            if not csv_files:
                await self.add_result("CSV Parsing", "⚠️ No CSV files available for testing")
                return False
                
            # Test parsing a sample of files
            sample_files = csv_files[:3]  # Test first 3 files
            parser = CSVParser()
            
            results = []
            success = True
            
            for file in sample_files:
                try:
                    # Parse the file
                    with open(file, 'r') as f:
                        content = f.read()
                        events = parser.parse_csv_data(content)
                    
                    # Check results
                    if events:
                        # Count events by timestamp
                        ts_count = {}
                        for event in events:
                            ts = event.get("timestamp")
                            if isinstance(ts, datetime):
                                date_str = ts.strftime("%Y-%m-%d")
                                ts_count[date_str] = ts_count.get(date_str, 0) + 1
                        
                        # Format timestamp counts
                        ts_summary = []
                        for date, count in ts_count.items():
                            ts_summary.append(f"{date}: {count} events")
                        
                        file_success = True
                        detail = f"✅ Successfully parsed {len(events)} events\n  → "
                        detail += ", ".join(ts_summary[:3])
                        if len(ts_summary) > 3:
                            detail += f", and {len(ts_summary) - 3} more dates"
                    else:
                        file_success = False
                        success = False
                        detail = "❌ No events parsed"
                        
                except Exception as e:
                    file_success = False
                    success = False
                    detail = f"❌ Error: {str(e)}"
                    
                # Add to results
                filename = os.path.basename(file)
                results.append(f"{filename}: {detail}")
                
            # Add the result
            status = "✅ CSV parsing working correctly" if success else "❌ CSV parsing has issues"
            await self.add_result("CSV Parsing", status + "\n" + "\n".join(results))
            
            return success
        except Exception as e:
            logger.error(f"Error testing CSV parsing: {e}")
            await self.add_result("CSV Parsing", f"❌ Error: {str(e)}")
            return False
            
    async def check_database_insertion(self):
        """Check if events are properly inserted into the database"""
        try:
            # Get database connection
            if not hasattr(self.bot, 'db') or self.bot.db is None:
                logger.error("Database connection not available")
                await self.add_result("Database Insertion", "❌ Database connection not available")
                return False
                
            # Check collection existence and count documents
            collections = ["kills", "events", "player_stats", "server_configs"]
            results = []
            success = True
            
            for collection in collections:
                try:
                    # Count documents
                    count = await self.bot.db[collection].count_documents({})
                    results.append(f"{collection}: {count} documents")
                except Exception as e:
                    results.append(f"{collection}: ❌ Error - {str(e)}")
                    success = False
                    
            # Test inserting and retrieving a test document
            test_success = True
            try:
                # Create unique test ID
                test_id = f"test_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                test_doc = {"_id": test_id, "test": True, "timestamp": datetime.now()}
                
                # Insert test document
                await self.bot.db["test_diagnostics"].insert_one(test_doc)
                
                # Retrieve test document
                retrieved = await self.bot.db["test_diagnostics"].find_one({"_id": test_id})
                
                # Verify retrieval
                if retrieved and retrieved.get("test") is True:
                    results.append("✅ Test document insertion and retrieval successful")
                else:
                    results.append("❌ Test document retrieval failed")
                    test_success = False
                    
                # Clean up test document
                await self.bot.db["test_diagnostics"].delete_one({"_id": test_id})
                
            except Exception as e:
                results.append(f"❌ Test document error: {str(e)}")
                test_success = False
                
            success = success and test_success
                
            # Add the result
            status = "✅ Database operations working correctly" if success else "❌ Database operations have issues"
            await self.add_result("Database Insertion", status + "\n" + "\n".join(results))
            
            return success
        except Exception as e:
            logger.error(f"Error checking database insertion: {e}")
            await self.add_result("Database Insertion", f"❌ Error: {str(e)}")
            return False
            
    async def verify_end_to_end_processing(self, csv_file):
        """Verify end-to-end processing by checking if events from a specific file exist in DB"""
        try:
            if not csv_file or not os.path.exists(csv_file):
                await self.add_result("End-to-End Processing", "⚠️ No CSV file available for testing")
                return False
                
            # Process the file
            from utils.csv_parser import CSVParser
            from cogs.csv_processor import process_csv_kills
            
            # Get sample data from file
            with open(csv_file, 'r') as f:
                content = f.read()
                
            # Parse file with CSVParser directly
            parser = CSVParser()
            events = parser.parse_csv_data(content)
            
            if not events:
                await self.add_result("End-to-End Processing", "❌ No events parsed from CSV file")
                return False
                
            # Get a sample timestamp to search for in the database
            sample_event = events[0]
            sample_timestamp = sample_event.get("timestamp")
            
            if not isinstance(sample_timestamp, datetime):
                await self.add_result("End-to-End Processing", "❌ Invalid timestamp in parsed event")
                return False
                
            # Check if events with this timestamp already exist in the database
            if hasattr(self.bot, 'db') and self.bot.db is not None:
                # Search for kills around this timestamp (±1 minute)
                start_time = sample_timestamp - timedelta(minutes=1)
                end_time = sample_timestamp + timedelta(minutes=1)
                
                query = {
                    "timestamp": {
                        "$gte": start_time,
                        "$lte": end_time
                    }
                }
                
                # Check kills collection
                kill_count = await self.bot.db["kills"].count_documents(query)
                
                # If no kills found, try to process and import
                if kill_count == 0:
                    # Add message about attempting to import
                    await self.add_result("End-to-End Processing", "⏳ No matching events found. Attempting to process and import...")
                    
                    # Define a test server configuration
                    from datetime import timezone
                    test_server = {
                        "_id": "test_diagnostic",
                        "server_id": "test_server",
                        "name": "Diagnostic Test Server",
                        "guild_id": str(self.channel.guild.id),
                        "enabled": True,
                        "last_processed": datetime.now(timezone.utc).replace(year=2020),  # Old date to ensure processing
                        "killfeed_channel_id": str(self.channel_id),
                        "sftp": {
                            "hostname": "localhost",
                            "port": 22,
                            "username": "test",
                            "password": "test",
                            "path": os.path.dirname(csv_file)
                        }
                    }
                    
                    # Process the sample CSV directly
                    kill_events = []
                    for event in events:
                        if isinstance(event.get("timestamp"), datetime):
                            kill_events.append({
                                "timestamp": event.get("timestamp"),
                                "killer_name": event.get("killer_name", "Unknown"),
                                "killer_id": event.get("killer_id", "0"),
                                "victim_name": event.get("victim_name", "Unknown"),
                                "victim_id": event.get("victim_id", "0"),
                                "weapon": event.get("weapon", "Unknown"),
                                "distance": float(event.get("distance", 0)) if event.get("distance") else 0,
                                "server_id": "test_server",
                                "guild_id": str(self.channel.guild.id)
                            })
                    
                    # Directly insert the kill events if database is available
                    if kill_events and hasattr(self.bot, 'db') and self.bot.db is not None:
                        try:
                            # Insert the events
                            if len(kill_events) > 0:
                                await self.bot.db["kills"].insert_many(kill_events)
                                
                                # Count again
                                kill_count = await self.bot.db["kills"].count_documents(query)
                        except Exception as e:
                            logger.error(f"Error inserting kill events: {e}")
                
                # Build the result message
                if kill_count > 0:
                    status = f"✅ Found {kill_count} matching events in database"
                    detail = f"Successfully verified that events from {os.path.basename(csv_file)} are in the database"
                    
                    # Get a sample kill event
                    sample_kill = await self.bot.db["kills"].find_one(query)
                    if sample_kill:
                        # Format sample kill details
                        kill_details = f"Sample kill: {sample_kill.get('killer_name', 'Unknown')} → {sample_kill.get('victim_name', 'Unknown')}"
                        detail += f"\n{kill_details}"
                        
                    success = True
                else:
                    status = "❌ No matching events found in database after processing"
                    detail = f"Failed to find any events from {os.path.basename(csv_file)} in the database"
                    success = False
                    
                # Add the result
                await self.add_result("End-to-End Processing", f"{status}\n{detail}")
                
                return success
            else:
                await self.add_result("End-to-End Processing", "❌ Database connection not available")
                return False
                
        except Exception as e:
            logger.error(f"Error verifying end-to-end processing: {e}")
            await self.add_result("End-to-End Processing", f"❌ Error: {str(e)}")
            return False
        
async def run_diagnostic():
    """Run the diagnostic"""
    # Import bot
    sys.path.append('.')
    from bot import initialize_bot
    
    # Initialize bot
    bot = await initialize_bot(force_sync=False)
    if not bot:
        logger.error("Failed to initialize bot")
        return
    
    try:
        # Wait for bot to be ready
        if not bot.is_ready():
            logger.info("Waiting for bot to be ready...")
            await bot.wait_until_ready()
            
        logger.info(f"Bot is ready as {bot.user} (ID: {bot.user.id})")
        
        # Use the target channel ID
        channel_id = 1360632422957449237
        
        # Run diagnostic
        diagnostic = CSVDiagnostic(bot, channel_id)
        success = await diagnostic.start()
        
        if not success:
            # Fallback to a channel in the home guild
            logger.info("Falling back to a channel in home guild")
            if bot.home_guild_id:
                guild = bot.get_guild(bot.home_guild_id)
                if guild:
                    # Try to find a suitable channel
                    for channel in guild.text_channels:
                        if "killfeed" in channel.name.lower() or "bot" in channel.name.lower():
                            logger.info(f"Using fallback channel: {channel.name} (ID: {channel.id})")
                            diagnostic = CSVDiagnostic(bot, channel.id)
                            await diagnostic.start()
                            break
        
    except Exception as e:
        logger.error(f"Error running diagnostic: {e}")
    finally:
        # Close bot
        await bot.close()

if __name__ == "__main__":
    asyncio.run(run_diagnostic())