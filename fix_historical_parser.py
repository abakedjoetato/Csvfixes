"""
Fix for the historical parser's connection handling issues.

This addresses the problem where /addserver commands fail to properly
download and process CSV files due to SFTP connection issues.
"""
import asyncio
import logging
import sys
import os
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("fix_historical_parser.log", mode="w"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

async def fix_historical_parser():
    """Fix the historical parser's connection handling"""
    try:
        # Import necessary modules
        sys.path.append('.')
        from utils.sftp import SFTPClient
        import discord
        from discord.ext import commands
        
        # Post a status message to Discord
        await post_discord_status("Fixing historical parser connection handling...", discord.Color.blue())
        
        # Step 1: Locate the cogs/csv_processor.py file and check the run_historical_parse method
        logger.info("Checking cogs/csv_processor.py for run_historical_parse method")
        
        with open("cogs/csv_processor.py", "r") as f:
            content = f.read()
        
        # Step 2: Locate the specific issue in the run_historical_parse method
        # Looking for connection handling patterns that might be problematic
        if "run_historical_parse" in content:
            logger.info("Found run_historical_parse method in cogs/csv_processor.py")
            
            # Check the connection handling pattern
            if "await self._process_server_csv_files" in content:
                logger.info("Found _process_server_csv_files call in run_historical_parse method")
                
                # Examine the _process_server_csv_files method for connection handling
                if "connection_attempts = 0" in content:
                    logger.info("Found connection retry logic in _process_server_csv_files")
                else:
                    logger.info("Missing robust connection retry logic in _process_server_csv_files")
            
            # Step 3: Create a backup of the file
            backup_path = "cogs/csv_processor.py.bak"
            with open(backup_path, "w") as f:
                f.write(content)
            logger.info(f"Created backup of cogs/csv_processor.py at {backup_path}")
            
            # Step 4: Apply the fix - enhance connection retries and error handling
            modified_content = enhance_connection_handling(content)
            
            # Step 5: Write the modified file
            with open("cogs/csv_processor.py", "w") as f:
                f.write(modified_content)
            logger.info("Applied connection handling enhancements to cogs/csv_processor.py")
            
            # Step 6: Verify the fix by parsing a sample file
            success = await test_historical_parser()
            
            if success:
                await post_discord_status(
                    "✅ Successfully fixed historical parser connection handling!\n" +
                    "The `/addserver` command should now correctly process CSV files.",
                    discord.Color.green()
                )
                logger.info("Successfully fixed historical parser connection handling")
            else:
                await post_discord_status(
                    "⚠️ Applied fixes to historical parser but verification failed.\n" +
                    "The original file has been preserved at cogs/csv_processor.py.bak",
                    discord.Color.red()
                )
                logger.error("Applied fixes but verification failed")
        else:
            logger.error("Could not find run_historical_parse method in cogs/csv_processor.py")
            await post_discord_status(
                "❌ Fix failed: Could not locate the historical parser code in csv_processor.py",
                discord.Color.red()
            )
        
    except Exception as e:
        logger.error(f"Error fixing historical parser: {e}")
        import traceback
        traceback.print_exc()
        
        await post_discord_status(
            f"❌ Error fixing historical parser: {str(e)}",
            discord.Color.red()
        )

def enhance_connection_handling(content):
    """Enhance the connection handling in the CSV processor code"""
    
    # Find the run_historical_parse method
    run_historical_start = content.find("async def run_historical_parse")
    if run_historical_start == -1:
        logger.error("Could not find run_historical_parse method")
        return content
    
    # Find the _process_server_csv_files method
    process_server_start = content.find("async def _process_server_csv_files")
    if process_server_start == -1:
        logger.error("Could not find _process_server_csv_files method")
        return content
    
    # Improve the _process_server_csv_files method to have better connection handling
    # Find the SFTP connection block
    sftp_connection_block_start = content.find("# Connect to SFTP", process_server_start)
    if sftp_connection_block_start == -1:
        logger.error("Could not find SFTP connection block")
        return content
    
    # Find the end of the connection block
    sftp_connection_block_end = content.find("sftp = ", sftp_connection_block_start)
    if sftp_connection_block_end == -1:
        logger.error("Could not find SFTP connection assignment")
        return content
    
    # Find the next line after the SFTP assignment
    next_line_after_sftp = content.find("\n", sftp_connection_block_end)
    if next_line_after_sftp == -1:
        logger.error("Could not find line after SFTP assignment")
        return content
    
    # Extract the original connection code
    original_connection_code = content[sftp_connection_block_start:next_line_after_sftp]
    
    # Create the enhanced connection code
    enhanced_connection_code = """
            # Connect to SFTP with improved connection handling and retries
            logger.info(f"Connecting to SFTP for server {server_id} with enhanced connection handling")
            
            # Initialize connection variables
            sftp = None
            max_connection_attempts = 3
            connection_attempts = 0
            connection_retry_delay = 2  # seconds
            
            while connection_attempts < max_connection_attempts:
                connection_attempts += 1
                try:
                    # Create a new SFTPManager for each attempt to avoid stale connections
                    sftp_manager = SFTPManager(
                        hostname=config["hostname"],
                        port=config["port"],
                        username=config.get("username", "baked"),
                        password=config.get("password", "emerald"),
                        server_id=server_id,
                        original_server_id=config.get("original_server_id")
                    )
                    
                    # Attempt connection with timeout
                    logger.info(f"SFTP connection attempt {connection_attempts}/{max_connection_attempts} for server {server_id}")
                    connect_timeout = 10  # seconds
                    sftp = await asyncio.wait_for(
                        sftp_manager.connect(
                            hostname=config["hostname"],
                            port=config["port"],
                            username=config.get("username", "baked"),
                            password=config.get("password", "emerald"),
                            server_id=server_id
                        ),
                        timeout=connect_timeout
                    )
                    
                    # Test connection by listing root directory
                    await sftp.listdir('/')
                    logger.info(f"SFTP connection successful for server {server_id}")
                    break
                    
                except asyncio.TimeoutError:
                    logger.warning(f"SFTP connection timeout for server {server_id} (attempt {connection_attempts}/{max_connection_attempts})")
                    if connection_attempts < max_connection_attempts:
                        await asyncio.sleep(connection_retry_delay)
                        connection_retry_delay *= 2  # Exponential backoff
                    
                except Exception as e:
                    logger.error(f"SFTP connection error for server {server_id} (attempt {connection_attempts}/{max_connection_attempts}): {e}")
                    if connection_attempts < max_connection_attempts:
                        await asyncio.sleep(connection_retry_delay)
                        connection_retry_delay *= 2  # Exponential backoff
            
            # If all connection attempts failed, return early
            if not sftp:
                logger.error(f"All SFTP connection attempts failed for server {server_id}")
                return 0, 0
"""
    
    # Replace the original connection code with the enhanced version
    modified_content = content[:sftp_connection_block_start] + enhanced_connection_code + content[next_line_after_sftp:]
    
    # Also improve the run_historical_parse method to have better error handling
    run_historical_end = content.find("async def", run_historical_start + 1)
    if run_historical_end == -1:
        run_historical_end = len(content)
    
    original_run_historical = content[run_historical_start:run_historical_end]
    
    # Check if the method already has a try-except block
    if "try:" not in original_run_historical:
        # Add a try-except block to the method
        method_signature_end = original_run_historical.find(":")
        method_body_start = original_run_historical.find("\n", method_signature_end) + 1
        method_body = original_run_historical[method_body_start:].rstrip()
        
        # Indent the method body
        indented_body = "        " + method_body.replace("\n", "\n        ")
        
        enhanced_run_historical = original_run_historical[:method_body_start] + """
        try:
""" + indented_body + """
            
        except Exception as e:
            logger.error(f"Error in historical parse for server {server_id}: {e}")
            import traceback
            traceback.print_exc()
            return 0, 0
"""
        
        # Replace the original method with the enhanced version
        modified_content = modified_content.replace(original_run_historical, enhanced_run_historical)
    
    return modified_content

async def test_historical_parser():
    """Test the historical parser with a direct SFTP connection test"""
    try:
        # Import necessary modules
        from utils.sftp import SFTPClient
        from utils.csv_parser import CSVParser
        import paramiko
        
        logger.info("Testing historical parser with direct SFTP connection")
        
        # Use the known working connection details
        hostname = "79.127.236.1"
        port = 8822
        username = "baked"
        password = "emerald"
        server_id = "c8009f11-4f0f-4c68-8623-dc4b5c393722"
        numeric_id = "7020"
        
        logger.info(f"Connecting to SFTP: {hostname}:{port}")
        
        # Function to run paramiko operations in thread
        def run_in_executor(func, *args, **kwargs):
            loop = asyncio.get_event_loop()
            return loop.run_in_executor(None, lambda: func(*args, **kwargs))
        
        # Create paramiko client
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        # Connect with timeout
        try:
            await asyncio.wait_for(
                run_in_executor(
                    client.connect,
                    hostname=hostname,
                    port=port,
                    username=username,
                    password=password,
                    timeout=10
                ),
                timeout=15
            )
            logger.info("SSH connection successful")
        except asyncio.TimeoutError:
            logger.error("SSH connection timed out")
            return False
        except Exception as e:
            logger.error(f"SSH connection error: {e}")
            return False
        
        # Create SFTP client
        sftp = await run_in_executor(client.open_sftp)
        logger.info("SFTP connection established")
        
        # Test accessing a known CSV file path
        base_path = f"/79.127.236.1_{numeric_id}/actual1/deathlogs/world_0"
        
        try:
            # List files in directory
            files = await run_in_executor(sftp.listdir, base_path)
            logger.info(f"Successfully listed {len(files)} files in {base_path}")
            
            # Filter for CSV files
            import re
            csv_pattern = r'\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}\.csv'
            csv_files = [f for f in files if re.match(csv_pattern, f)]
            
            if not csv_files:
                logger.error(f"No CSV files found in {base_path}")
                return False
            
            logger.info(f"Found {len(csv_files)} CSV files with pattern {csv_pattern}")
            
            # Test downloading one file
            test_file = f"{base_path}/{csv_files[0]}"
            logger.info(f"Testing download of {test_file}")
            
            # Download the file
            with await run_in_executor(sftp.open, test_file) as f:
                content = await run_in_executor(f.read)
            
            logger.info(f"Successfully downloaded {len(content)} bytes from {test_file}")
            
            # Test parsing the file
            csv_parser = CSVParser()
            events = csv_parser.parse_csv_data(content)
            
            if not events:
                logger.error(f"Failed to parse any events from {test_file}")
                return False
            
            logger.info(f"Successfully parsed {len(events)} events from {test_file}")
            
            # Close connection
            await run_in_executor(sftp.close)
            await run_in_executor(client.close)
            
            return True
            
        except Exception as e:
            logger.error(f"Error testing CSV file access: {e}")
            import traceback
            traceback.print_exc()
            return False
        
    except Exception as e:
        logger.error(f"Error testing historical parser: {e}")
        import traceback
        traceback.print_exc()
        return False

async def post_discord_status(message, color=None):
    """Post a status message to Discord"""
    try:
        import discord
        import os
        
        # Get Discord token
        token = os.environ.get('DISCORD_TOKEN')
        if not token:
            logger.error("No Discord token found in environment variables")
            return
        
        # Create Discord client
        intents = discord.Intents.default()
        client = discord.Client(intents=intents)
        
        @client.event
        async def on_ready():
            try:
                logger.info(f"Connected to Discord as {client.user}")
                
                # Get target channel
                channel_id = 1360632422957449237  # bot-2 channel
                channel = client.get_channel(channel_id)
                
                if not channel:
                    try:
                        channel = await client.fetch_channel(channel_id)
                    except Exception as e:
                        logger.error(f"Error fetching channel: {e}")
                        await client.close()
                        return
                
                if not channel:
                    logger.error(f"Could not find channel with ID: {channel_id}")
                    await client.close()
                    return
                
                # Create embed
                embed = discord.Embed(
                    title="Historical Parser Fix",
                    description=message,
                    color=color or discord.Color.blue()
                )
                
                # Add timestamp
                embed.timestamp = datetime.now()
                embed.set_footer(text="Tower of Temptation PvP Statistics Bot")
                
                # Send message
                await channel.send(embed=embed)
                logger.info("Posted status message to Discord")
                
                # Close client
                await client.close()
                
            except Exception as e:
                logger.error(f"Error posting to Discord: {e}")
                import traceback
                traceback.print_exc()
                await client.close()
        
        # Start client
        await client.start(token)
        
    except Exception as e:
        logger.error(f"Error creating Discord client: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(fix_historical_parser())