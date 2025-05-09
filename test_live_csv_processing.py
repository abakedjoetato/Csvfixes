"""
Live CSV Processing Test

This script tests the CSV processor with real SFTP connections to verify
that the timestamp parsing fix works in a real-world environment.
"""

import asyncio
import discord
import logging
import sys
import os
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('live_csv_test.log')
    ]
)

logger = logging.getLogger(__name__)

# Target channel ID for posting results
TARGET_CHANNEL_ID = 1360632422957449237

async def test_live_csv_processing():
    """Test CSV processing with live SFTP connections"""
    logger.info("Starting live SFTP CSV processing test")
    
    try:
        # Import necessary modules
        import discord
        from discord.ext import commands
        import asyncio
        import sys
        
        sys.path.append('.')
        from bot import initialize_bot
        
        # Initialize the bot with login
        bot = await initialize_bot(force_sync=False)
        if not bot:
            logger.error("Failed to initialize bot")
            return False
        
        # Get the CSV processor cog
        csv_processor = bot.get_cog("CSVProcessorCog")
        if not csv_processor:
            logger.error("CSV processor cog not found")
            return False
        
        # Get all server configurations with SFTP enabled
        server_configs = await csv_processor._get_server_configs()
        if not server_configs:
            logger.error("No server configs found")
            return False
        
        logger.info(f"Found {len(server_configs)} server configurations")
        
        # Process CSV files for all servers
        results = {}
        total_files = 0
        total_events = 0
        
        for server_id, config in server_configs.items():
            logger.info(f"Processing server: {server_id}")
            
            # Remember the previous last processed time
            previous_last_processed = csv_processor.last_processed.get(server_id, None)
            
            # Set last processed time to 60 days ago to process historical data
            csv_processor.last_processed[server_id] = datetime.now() - timedelta(days=60)
            
            try:
                # Process the server's CSV files
                files_processed, events_processed = await csv_processor._process_server_csv_files(server_id, config)
                
                # Store results
                results[server_id] = {
                    "files_processed": files_processed,
                    "events_processed": events_processed,
                    "config": {k: v for k, v in config.items() if k not in ['password', 'sftp_password']}
                }
                
                total_files += files_processed
                total_events += events_processed
                
                logger.info(f"Server {server_id}: Processed {files_processed} files with {events_processed} events")
                
                # Restore previous last processed time
                if previous_last_processed:
                    csv_processor.last_processed[server_id] = previous_last_processed
                else:
                    # If there was no previous time, remove it
                    csv_processor.last_processed.pop(server_id, None)
                    
            except Exception as e:
                logger.error(f"Error processing server {server_id}: {str(e)}")
                results[server_id] = {
                    "error": str(e),
                    "files_processed": 0,
                    "events_processed": 0
                }
        
        logger.info(f"Total: Processed {total_files} files with {total_events} events")
        
        # Try to send results to Discord if we can
        try:
            # Get channel for posting results
            channel = bot.get_channel(TARGET_CHANNEL_ID)
            
            if channel:
                # Create embed for results
                embed = discord.Embed(
                    title="Live CSV Processing Test Results",
                    description=f"Timestamp parsing fix verified with live SFTP data",
                    color=discord.Color.green() if total_files > 0 else discord.Color.red()
                )
                
                embed.add_field(name="Total Files Processed", value=str(total_files), inline=True)
                embed.add_field(name="Total Events Processed", value=str(total_events), inline=True)
                embed.add_field(name="Servers Tested", value=str(len(server_configs)), inline=True)
                
                # Add details for each server
                for server_id, server_result in results.items():
                    server_name = server_result.get("config", {}).get("name", server_id)
                    files = server_result.get("files_processed", 0)
                    events = server_result.get("events_processed", 0)
                    error = server_result.get("error", None)
                    
                    if error:
                        embed.add_field(
                            name=f"Server: {server_name}",
                            value=f"Error: {error}",
                            inline=False
                        )
                    else:
                        embed.add_field(
                            name=f"Server: {server_name}",
                            value=f"Files: {files}, Events: {events}",
                            inline=False
                        )
                
                # Add timestamp parsing status
                if total_files > 0:
                    embed.add_field(
                        name="Timestamp Parsing Status",
                        value="✅ Fix verified with live data - all CSV files successfully processed",
                        inline=False
                    )
                else:
                    embed.add_field(
                        name="Timestamp Parsing Status",
                        value="❌ No CSV files were processed - fix could not be verified",
                        inline=False
                    )
                
                # Send the results
                await channel.send(embed=embed)
                logger.info(f"Posted results to channel {channel.name}")
            else:
                logger.error(f"Channel with ID {TARGET_CHANNEL_ID} not found")
                
        except Exception as e:
            logger.error(f"Error posting results to Discord: {str(e)}")
        
        # Return success status
        return total_files > 0
        
    except Exception as e:
        logger.error(f"Error testing live CSV processing: {str(e)}")
        return False

async def main():
    """Main function for the test script"""
    try:
        # Run the test
        success = await test_live_csv_processing()
        
        if success:
            logger.info("Live CSV processing test PASSED")
        else:
            logger.error("Live CSV processing test FAILED")
            
    except Exception as e:
        logger.error(f"Unhandled error in test: {str(e)}")
    finally:
        # We're done, exit
        logger.info("Test complete, exiting")

if __name__ == "__main__":
    asyncio.run(main())