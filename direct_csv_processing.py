"""
Direct CSV processing command script

This script directly triggers the CSV processing command in the running bot.
"""
import asyncio
import discord
import logging
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

async def main():
    """Main function"""
    # Get the Discord bot
    from bot import initialize_bot
    
    bot = await initialize_bot(force_sync=False)
    if not bot:
        logger.error("Failed to initialize bot")
        return
    
    try:
        # Wait for the bot to be ready
        await bot.wait_until_ready()
        logger.info(f"Bot is ready as {bot.user}")
        
        # Get the CSV processor cog
        csv_processor = bot.get_cog("CSVProcessorCog")
        if not csv_processor:
            logger.error("CSV processor cog not found")
            return
        
        # Get the ID of the Emerald Servers guild
        target_guild_id = 1219706687980568769
        target_guild = bot.get_guild(target_guild_id)
        if not target_guild:
            logger.error(f"Target guild with ID {target_guild_id} not found")
            return
        
        # Find a suitable channel to post results in
        target_channel = None
        for channel in target_guild.text_channels:
            if "bot" in channel.name.lower() or "csv" in channel.name.lower() or "killfeed" in channel.name.lower():
                target_channel = channel
                logger.info(f"Found channel: #{channel.name} (ID: {channel.id})")
                break
        
        if not target_channel:
            # Fall back to the first text channel
            if target_guild.text_channels:
                target_channel = target_guild.text_channels[0]
                logger.info(f"Using fallback channel: #{target_channel.name} (ID: {target_channel.id})")
        
        if not target_channel:
            logger.error("No suitable channel found")
            return
        
        # Create embedded message for progress reporting
        embed = discord.Embed(
            title="CSV Processing Test",
            description="Testing direct CSV processing with YYYY.MM.DD-HH.MM.SS format",
            color=discord.Color.blue()
        )
        message = await target_channel.send(embed=embed)
        
        # Update the message with status
        embed.add_field(name="Status", value="⏳ Starting CSV processing...", inline=False)
        await message.edit(embed=embed)
        
        # Get server configs
        server_configs = await csv_processor._get_server_configs()
        if not server_configs:
            embed.add_field(name="Error", value="❌ No server configurations found", inline=False)
            await message.edit(embed=embed)
            logger.error("No server configurations found")
            return
        
        # Show server configs
        server_info = []
        for config in server_configs:
            server_id = config.get("server_id", "Unknown")
            name = config.get("name", "Unknown")
            guild_id = config.get("guild_id", "Unknown")
            server_info.append(f"- {name} (ID: {server_id}, Guild: {guild_id})")
        
        embed.add_field(
            name="Server Configs", 
            value=f"Found {len(server_configs)} server configurations:\n" + "\n".join(server_info[:3]) +
                  (f"\n... and {len(server_configs) - 3} more" if len(server_configs) > 3 else ""),
            inline=False
        )
        await message.edit(embed=embed)
        
        # Get the first server config
        server_config = server_configs[0]
        server_id = server_config.get("server_id")
        server_name = server_config.get("name", "Unknown")
        
        # Update the message with processing status
        embed.add_field(
            name="Processing", 
            value=f"⏳ Processing server: {server_name} (ID: {server_id})", 
            inline=False
        )
        await message.edit(embed=embed)
        
        # Force last_processed to an old date to ensure all files are processed
        from datetime import datetime, timezone, timedelta
        csv_processor.last_processed[server_id] = datetime.now(timezone.utc) - timedelta(days=60)
        
        # Process the server's CSV files
        result = await csv_processor._process_server_csv_files(server_config)
        
        # Check results
        if isinstance(result, dict):
            processed_files = result.get("processed_files", 0)
            total_kills = result.get("total_kills", 0)
            status = f"✅ Processed {processed_files} files with {total_kills} total kills"
        else:
            status = f"❌ Failed with result: {result}"
        
        # Update the message with results
        embed.add_field(name="Results", value=status, inline=False)
        await message.edit(embed=embed)
        
        # Verify timestamp parsing
        embed.add_field(
            name="Timestamp Verification", 
            value="⏳ Verifying timestamp parsing with sample CSV data...", 
            inline=False
        )
        await message.edit(embed=embed)
        
        # Test timestamp parsing with direct sample
        from utils.csv_parser import CSVParser
        parser = CSVParser()
        
        # Sample data with the YYYY.MM.DD-HH.MM.SS format
        sample_data = "2025.05.09-11.36.58;TestKiller;12345;TestVictim;67890;AK47;100;PC"
        events = parser.parse_csv_data(sample_data)
        
        if events and len(events) > 0:
            event = events[0]
            timestamp = event.get("timestamp")
            if isinstance(timestamp, datetime):
                timestamp_status = f"✅ Successfully parsed timestamp: {timestamp}"
            else:
                timestamp_status = f"❌ Failed to parse timestamp: {timestamp}"
        else:
            timestamp_status = "❌ Failed to parse test event"
        
        # Update timestamp verification
        embed.set_field_at(
            index=-1,  # Last field
            name="Timestamp Verification",
            value=timestamp_status,
            inline=False
        )
        
        # Add test with real CSV file if available
        if hasattr(result, "csv_files") and result.csv_files:
            real_file = result.csv_files[0]
            embed.add_field(
                name="Real CSV File Test",
                value=f"Testing with real file: {real_file}",
                inline=False
            )
            await message.edit(embed=embed)
            
            # We'll get results from the previous processing
        
        # Final status
        embed.description = "✅ CSV Processing Test Complete"
        embed.color = discord.Color.green()
        await message.edit(embed=embed)
        
    except Exception as e:
        logger.error(f"Error: {e}")
        # Try to post error if message exists
        try:
            if 'message' in locals() and message:
                embed.add_field(name="Error", value=f"❌ {str(e)}", inline=False)
                embed.color = discord.Color.red()
                await message.edit(embed=embed)
        except:
            pass
    finally:
        await bot.close()

if __name__ == "__main__":
    asyncio.run(main())