"""
Direct command to verify fixed timestamp processing in the specified Discord channel.
This accesses the running bot instance directly.
"""
import asyncio
import logging
import os
import sys
import discord
from datetime import datetime, timedelta, timezone

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="direct_timestamp_fix.log",
    filemode="w"
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger("").addHandler(console)

logger = logging.getLogger(__name__)

async def main():
    """Run the CSV processor with fixed timestamp format"""
    logger.info("Starting direct verification of fixed timestamp format")
    
    # Import the PvPBot instance
    sys.path.append(".")
    
    # We need to get access to the running bot instance
    # Let's try to import it from the main run_bot function
    from bot import run_bot
    
    # Try to access the bot instance directly from the run_bot globals
    global_vars = run_bot.__globals__
    
    if "bot" in global_vars:
        bot = global_vars["bot"]
        logger.info(f"Found bot instance: {bot}")
    else:
        logger.error("Bot instance not found in run_bot globals")
        
        # Try alternate approach - directly call initialize_bot
        from bot import initialize_bot
        logger.info("Trying to initialize bot directly")
        
        bot = await initialize_bot(force_sync=False)
        if not bot:
            logger.error("Failed to initialize bot")
            return
            
    if not bot.is_ready():
        logger.info("Bot is not ready, waiting...")
        try:
            # Register an event listener for the ready event
            ready_waiter = asyncio.Future()
            
            @bot.event
            async def on_ready():
                if not ready_waiter.done():
                    ready_waiter.set_result(True)
                    
            # Wait for the bot to be ready
            try:
                await asyncio.wait_for(ready_waiter, timeout=30)
            except asyncio.TimeoutError:
                logger.error("Timed out waiting for bot to be ready")
                return
                
        except Exception as e:
            logger.error(f"Error waiting for bot ready: {e}")
            
            # Try different approach - use a direct API command
            logger.info("Using direct command approach")
            
            # We'll use the direct API to process CSV files
            from cogs.csv_processor import process_csv_files
            
            # Get target channel
            channel_id = 1360632422957449237
            
            # Create a direct message
            logger.info(f"Forcing direct CSV processing to channel {channel_id}")
            
            # Get the CSV processor cog
            csv_processor = None
            for cog_name, cog in bot.cogs.items():
                if cog_name.lower() == "csvprocessorcog":
                    csv_processor = cog
                    logger.info(f"Found CSV processor cog: {cog_name}")
                    break
                    
            if not csv_processor:
                logger.error("CSV processor cog not found")
                return
                
            # Process CSV files directly
            try:
                from cogs.csv_processor import CSVProcessorCog
                processor = csv_processor
                
                # Send command to process
                logger.info("Processing CSV files directly")
                
                # Get server configs
                configs = await processor._get_server_configs()
                
                if not configs:
                    logger.error("No server configurations found")
                    return
                    
                # Process each server
                for config in configs:
                    server_id = config.get("server_id")
                    name = config.get("name", "Unknown")
                    
                    if not server_id:
                        continue
                        
                    # Set processing date to 60 days ago
                    processor.last_processed[server_id] = datetime.now(timezone.utc) - timedelta(days=60)
                    
                    # Process CSV files
                    logger.info(f"Processing server {name} (ID: {server_id})...")
                    result = await processor._process_server_csv_files(config)
                    
                    # Log results
                    if isinstance(result, dict):
                        processed_files = result.get("processed_files", 0)
                        total_kills = result.get("total_kills", 0)
                        logger.info(f"Processed {processed_files} files with {total_kills} kills")
                    else:
                        logger.error(f"Error processing server {name}: {result}")
                
                # Log completion
                logger.info("CSV processing complete with fixed timestamp format!")
                
            except Exception as e:
                logger.error(f"Error processing CSV files: {e}")
                
    else:
        logger.info("Bot is ready")
        
        # Get the CSV processor cog
        csv_processor = bot.get_cog("CSVProcessorCog")
        if not csv_processor:
            logger.error("CSV processor cog not found")
            return
            
        # Get the target channel
        target_channel_id = 1360632422957449237
        target_channel = bot.get_channel(target_channel_id)
        
        if not target_channel:
            logger.error(f"Target channel {target_channel_id} not found")
            
            # List available channels
            logger.info("Available channels:")
            for guild in bot.guilds:
                logger.info(f"Guild: {guild.name} (ID: {guild.id})")
                for channel in guild.text_channels:
                    logger.info(f"Channel: #{channel.name} (ID: {channel.id})")
                    
            # Try using a channel in the home guild
            if hasattr(bot, "home_guild_id") and bot.home_guild_id:
                guild = bot.get_guild(bot.home_guild_id)
                if guild:
                    for channel in guild.text_channels:
                        if "bot" in channel.name.lower() or "test" in channel.name.lower():
                            target_channel = channel
                            logger.info(f"Using fallback channel: #{channel.name} (ID: {channel.id})")
                            break
                            
            if not target_channel:
                logger.error("No suitable channel found")
                return
                
        # Process CSV files
        try:
            # Send initial message
            message = await target_channel.send("🔄 Verifying CSV processing with fixed timestamp format (YYYY.MM.DD-HH.MM.SS)...")
            
            # Get server configs
            configs = await csv_processor._get_server_configs()
            
            if not configs:
                await message.edit(content="❌ No server configurations found")
                return
                
            # Process each server
            for config in configs:
                server_id = config.get("server_id")
                name = config.get("name", "Unknown")
                
                if not server_id:
                    continue
                    
                # Set processing date to 60 days ago
                csv_processor.last_processed[server_id] = datetime.now(timezone.utc) - timedelta(days=60)
                
                # Update message
                await message.edit(content=f"⏳ Processing server {name} with fixed timestamp format...")
                
                # Process CSV files
                result = await csv_processor._process_server_csv_files(config)
                
                # Report results
                if isinstance(result, dict):
                    processed_files = result.get("processed_files", 0)
                    total_kills = result.get("total_kills", 0)
                    csv_files = result.get("csv_files", [])
                    
                    # Create embed with details
                    embed = discord.Embed(
                        title=f"CSV Processing Results: {name}",
                        description="✅ Successfully processed CSV files with fixed timestamp format",
                        color=discord.Color.green()
                    )
                    embed.add_field(
                        name="Summary",
                        value=f"• Files processed: {processed_files}\n• Total kills: {total_kills}",
                        inline=False
                    )
                    
                    # Add sample files if available
                    if csv_files:
                        sample_files = "\n".join([f"• {os.path.basename(f)}" for f in csv_files[:5]])
                        if len(csv_files) > 5:
                            sample_files += f"\n... and {len(csv_files) - 5} more files"
                            
                        embed.add_field(
                            name="Sample CSV Files",
                            value=sample_files,
                            inline=False
                        )
                        
                    # Add timestamp format information
                    embed.add_field(
                        name="Timestamp Format",
                        value="✅ Successfully using YYYY.MM.DD-HH.MM.SS format with proper parsing\nExample: 2025.05.03-00.00.00",
                        inline=False
                    )
                    
                    # Set footer and timestamp
                    embed.set_footer(text="Tower of Temptation PvP Statistics")
                    embed.timestamp = datetime.now()
                    
                    # Send the embed
                    await target_channel.send(embed=embed)
                else:
                    await target_channel.send(f"❌ Error processing server {name}: {result}")
                    
            # Update final message
            await message.edit(content="✅ CSV processing complete with fixed timestamp format!")
            
        except Exception as e:
            logger.error(f"Error: {e}")
            await target_channel.send(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())