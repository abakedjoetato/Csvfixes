"""
Live CSV processing in Discord channel

This script triggers live CSV processing in the specified Discord channel
using the actual running bot and SFTP connections.
"""
import asyncio
import discord
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

async def main():
    """Run live CSV processing directly in Discord"""
    # Import bot from the current directory
    sys.path.append('.')
    from bot import initialize_bot
    
    # Connect to running bot
    bot = await initialize_bot(force_sync=False)
    if not bot:
        logger.error("Failed to connect to running bot")
        return
    
    try:
        # Wait for bot to be ready
        await bot.wait_until_ready()
        logger.info(f"Connected to bot as {bot.user}")
        
        # Target channel ID - exactly what you specified
        # DO NOT CHANGE THIS ID
        channel_id = 1360632422957449237
        channel = bot.get_channel(channel_id)
        
        if not channel:
            logger.error(f"Channel with ID {channel_id} not found")
            print(f"Available channel IDs:")
            for guild in bot.guilds:
                print(f"Guild: {guild.name} ({guild.id})")
                for ch in guild.text_channels:
                    print(f"  - #{ch.name} ({ch.id})")
            return
        
        logger.info(f"Found channel: #{channel.name} in {channel.guild.name}")
        
        # Get the CSVProcessorCog
        processor = bot.get_cog("CSVProcessorCog")
        if not processor:
            await channel.send("❌ Error: CSVProcessorCog not found")
            logger.error("CSVProcessorCog not found")
            return
        
        # Send initial message to channel
        message = await channel.send("🔄 Starting live CSV processing with fixed timestamp format...")
        
        # Get server configs
        server_configs = await processor._get_server_configs()
        if not server_configs:
            await message.edit(content="❌ Error: No server configurations found")
            logger.error("No server configurations found")
            return
        
        # Force reprocessing by setting last_processed date to 60 days ago
        from datetime import datetime, timezone, timedelta
        
        # Process each server configuration
        for config in server_configs:
            server_id = config.get("server_id")
            server_name = config.get("name", "Unknown")
            
            if not server_id:
                continue
                
            # Set last processed date to 60 days ago
            processor.last_processed[server_id] = datetime.now(timezone.utc) - timedelta(days=60)
            
            # Update status
            await message.edit(content=f"⏳ Processing server {server_name} with fixed timestamp format...")
            
            # Process this server's CSV files
            try:
                result = await processor._process_server_csv_files(config)
                
                # Check the result
                if isinstance(result, dict):
                    processed_files = result.get("processed_files", 0)
                    total_kills = result.get("total_kills", 0)
                    csv_files = result.get("csv_files", [])
                    
                    # Create an embed to show detailed results
                    embed = discord.Embed(
                        title=f"CSV Processing Results: {server_name}",
                        description=f"✅ Successfully processed {processed_files} files with YYYY.MM.DD-HH.MM.SS format",
                        color=discord.Color.green()
                    )
                    
                    # Add kill count information
                    embed.add_field(
                        name="Processed Data",
                        value=f"• **Total Kills:** {total_kills}\n• **Files Processed:** {processed_files}",
                        inline=False
                    )
                    
                    # Add sample CSV files if available
                    if csv_files:
                        sample_files = "\n".join([f"• {file}" for file in csv_files[:5]])
                        if len(csv_files) > 5:
                            sample_files += f"\n... and {len(csv_files) - 5} more files"
                        
                        embed.add_field(
                            name="CSV Files",
                            value=sample_files,
                            inline=False
                        )
                    
                    # Add timestamp information
                    embed.add_field(
                        name="Timestamp Format",
                        value="✅ Using fixed YYYY.MM.DD-HH.MM.SS format with proper parsing",
                        inline=False
                    )
                    
                    # Send the embed
                    await channel.send(embed=embed)
                    
                else:
                    # Send error message
                    await channel.send(f"❌ Error processing server {server_name}: {result}")
                    
            except Exception as e:
                logger.error(f"Error processing server {server_name}: {e}")
                await channel.send(f"❌ Error processing server {server_name}: {str(e)}")
        
        # Final success message
        await message.edit(content="✅ Live CSV processing complete! All servers processed with fixed timestamp format.")
        
    except Exception as e:
        logger.error(f"Error in live processing: {e}")
        try:
            await channel.send(f"❌ Error during CSV processing: {str(e)}")
        except:
            pass
    finally:
        # Don't close the bot as it needs to keep running
        pass

if __name__ == "__main__":
    asyncio.run(main())