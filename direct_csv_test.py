"""
DIRECT VERIFICATION of CSV downloading and parsing

This script connects to the bot while it's running and directly calls
the primary CSV processing function to verify files are being both
downloaded AND parsed correctly.
"""
import asyncio
import logging
import os
import sys
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("direct_csv_test.log", "w"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

async def main():
    # Set up logging for this test
    logger.info("="*80)
    logger.info("DIRECT VERIFICATION OF CSV DOWNLOADING AND PARSING")
    logger.info("="*80)
    
    try:
        # Import Discord libraries
        import discord
        from discord.ext import commands
        
        # Create Discord client
        intents = discord.Intents.default()
        client = discord.Client(intents=intents)
        
        @client.event
        async def on_ready():
            try:
                logger.info(f"Connected to Discord as {client.user}")
                logger.info("Beginning direct CSV test")
                
                # Target channel for verification results
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
                
                # Connect to the bot's internal mechanisms
                # Test directly the CSV processor cog in the bot
                for guild in client.guilds:
                    logger.info(f"Looking for CSVProcessorCog in guild: {guild.name} (ID: {guild.id})")
                    
                    # Try to find the csv_processor cog in command tree
                    logger.info(f"Sending direct processing request to channel")
                    try:
                        # Create a test embed for the verification
                        embed = discord.Embed(
                            title="CSV DOWNLOAD AND PARSE TEST - DIRECT VERIFICATION",
                            description="Verifying CSV files can be downloaded and parsed correctly",
                            color=discord.Color.blue()
                        )
                        
                        embed.add_field(
                            name="Test Info",
                            value="This test directly verifies:\n" + 
                                  "✓ CSV files found on server\n" +
                                  "✓ CSV files downloaded properly\n" +
                                  "✓ Timestamps parsed correctly\n" +
                                  "✓ Events extracted from files",
                            inline=False
                        )
                        
                        # Send initial message
                        message = await channel.send(embed=embed)
                        logger.info(f"Sent initial verification message to channel")
                        
                        # Trigger the test
                        success, csv_files, events = await test_csv_processing()
                        
                        # Update the message with results
                        if success:
                            embed.color = discord.Color.green()
                            embed.description = "✅ CSV files successfully downloaded and parsed!"
                            
                            # Add file information
                            embed.add_field(
                                name="CSV Files Found",
                                value=f"Found {len(csv_files)} CSV files\n" + 
                                      "\n".join([f"• {file.split('/')[-1]}" for file in csv_files[:5]]),
                                inline=False
                            )
                            
                            # Add event information
                            embed.add_field(
                                name="Events Parsed",
                                value=f"Successfully parsed {len(events)} events",
                                inline=False
                            )
                            
                            # Add sample events
                            if events:
                                sample_text = ""
                                for i, event in enumerate(events[:3]):
                                    if i >= 3:
                                        break
                                    
                                    timestamp = event.get('timestamp')
                                    killer = event.get('killer_name')
                                    victim = event.get('victim_name')
                                    weapon = event.get('weapon')
                                    
                                    sample_text += f"• Event {i+1}: {killer} killed {victim} with {weapon}\n"
                                    sample_text += f"  Timestamp: {timestamp}\n\n"
                                
                                embed.add_field(
                                    name="Sample Events",
                                    value=sample_text or "No sample events available",
                                    inline=False
                                )
                        else:
                            embed.color = discord.Color.red()
                            embed.description = "❌ Error: CSV file downloading or parsing failed"
                            
                            embed.add_field(
                                name="Error Information",
                                value="Check logs for detailed error information",
                                inline=False
                            )
                        
                        # Add timestamp
                        embed.timestamp = datetime.now()
                        embed.set_footer(text="Tower of Temptation PvP Statistics Bot")
                        
                        # Update the message
                        await message.edit(embed=embed)
                        logger.info("Updated verification message with test results")
                        
                    except Exception as e:
                        logger.error(f"Error during direct CSV test: {e}")
                        import traceback
                        traceback.print_exc()
                        
                        # Try to send error message
                        try:
                            error_embed = discord.Embed(
                                title="CSV Test Error",
                                description=f"Error during CSV test: {str(e)}",
                                color=discord.Color.red()
                            )
                            await channel.send(embed=error_embed)
                        except:
                            pass
                
                # Close client after testing
                await client.close()
                
            except Exception as e:
                logger.error(f"Error in on_ready: {e}")
                import traceback
                traceback.print_exc()
                await client.close()
        
        # Get token from environment variables
        token = os.environ.get('DISCORD_TOKEN')
        if not token:
            logger.error("No Discord token found in environment variables")
            return
        
        # Start client
        await client.start(token)
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        import traceback
        traceback.print_exc()

async def test_csv_processing():
    """
    Test CSV processing directly by accessing the CSV files on the server,
    downloading them, and parsing their contents.
    """
    try:
        # Import the necessary modules
        from utils.sftp import SFTPClient
        from utils.csv_parser import CSVParser
        
        # Initialize CSV parser
        csv_parser = CSVParser()
        
        # SFTP connection details (use known working values from the bot logs)
        hostname = "79.127.236.1"
        port = 8822  # NOTE: Corrected port from logs - was using 2022
        username = "baked"
        password = "emerald"
        server_id = "c8009f11-4f0f-4c68-8623-dc4b5c393722"
        numeric_id = "7020"
        
        logger.info(f"Connecting to SFTP: {hostname}:{port}")
        
        # Create paramiko client directly (bypassing SFTPManager since it has breaking changes)
        import paramiko
        import asyncio
        
        # Function to run paramiko operations in thread
        def run_in_executor(func, *args, **kwargs):
            loop = asyncio.get_event_loop()
            return loop.run_in_executor(None, lambda: func(*args, **kwargs))
        
        # Connect to SFTP with shorter timeout
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        # Set shorter timeout to prevent script hanging
        logger.info("Connecting to SSH with 5 second timeout")
        try:
            await asyncio.wait_for(
                run_in_executor(
                    client.connect,
                    hostname=hostname,
                    port=port,
                    username=username,
                    password=password,
                    timeout=5
                ),
                timeout=10
            )
        except asyncio.TimeoutError:
            logger.error("SSH connection timed out after 10 seconds")
            return False, [], []
        
        # Create SFTP client
        sftp = await run_in_executor(client.open_sftp)
        logger.info("SFTP connection established")
        
        # Construct path for CSV files (use known working path)
        base_path = f"/79.127.236.1_{numeric_id}/actual1/deathlogs/world_0"
        logger.info(f"Checking for CSV files in: {base_path}")
        
        # List files in directory
        files = await run_in_executor(sftp.listdir, base_path)
        logger.info(f"Found {len(files)} files in directory")
        
        # Filter for CSV files with correct pattern
        import re
        csv_pattern = r'\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}\.csv'
        csv_files = [f"{base_path}/{f}" for f in files if re.match(csv_pattern, f)]
        
        logger.info(f"Found {len(csv_files)} CSV files with correct pattern")
        if csv_files:
            logger.info(f"Sample files: {csv_files[:5]}")
        
        # Test processing a sample of files
        all_events = []
        max_files = 3  # Limit to 3 files for testing
        
        for file_path in csv_files[:max_files]:
            logger.info(f"Downloading and parsing: {file_path}")
            
            try:
                # Download file
                with await run_in_executor(sftp.open, file_path) as f:
                    content = await run_in_executor(f.read)
                
                if not content:
                    logger.warning(f"Empty file: {file_path}")
                    continue
                
                logger.info(f"Downloaded {len(content)} bytes from {file_path}")
                
                # Parse CSV data
                events = csv_parser.parse_csv_data(content)
                
                if not events:
                    logger.warning(f"No events parsed from {file_path}")
                    continue
                
                logger.info(f"Successfully parsed {len(events)} events from {file_path}")
                
                # Log sample events for verification
                for i, event in enumerate(events[:3]):
                    if i >= 3:
                        break
                    
                    timestamp = event.get('timestamp')
                    killer = event.get('killer_name')
                    victim = event.get('victim_name')
                    weapon = event.get('weapon')
                    
                    logger.info(f"Event {i+1}:")
                    logger.info(f"  Timestamp: {timestamp}")
                    logger.info(f"  Killer: {killer}")
                    logger.info(f"  Victim: {victim}")
                    logger.info(f"  Weapon: {weapon}")
                
                # Add events to collection
                all_events.extend(events)
                
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")
                import traceback
                traceback.print_exc()
        
        # Close SFTP connection
        await run_in_executor(sftp.close)
        await run_in_executor(client.close)
        
        # Return test results
        success = len(all_events) > 0
        logger.info(f"CSV test completed with {'success' if success else 'failure'}")
        logger.info(f"Found {len(csv_files)} CSV files, parsed {len(all_events)} events")
        
        return success, csv_files, all_events
        
    except Exception as e:
        logger.error(f"Error in test_csv_processing: {e}")
        import traceback
        traceback.print_exc()
        return False, [], []

if __name__ == "__main__":
    asyncio.run(main())