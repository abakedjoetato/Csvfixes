"""
Direct command to post to specific channel: 1360632422957449237
"""
import discord
import asyncio
import sys
import logging
from datetime import datetime, timezone, timedelta
import os
from typing import Dict, Any, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Connect to the bot and send a command
async def run_command():
    try:
        # Import the necessary modules
        sys.path.append('.')
        import importlib
        import bot as bot_module
        
        # Get the bot client
        client = bot_module.client
        
        # Check if the client is logged in
        if not client.is_ready():
            logger.error("Bot is not ready")
            return
            
        # Get the target channel
        channel_id = 1360632422957449237
        channel = client.get_channel(channel_id)
        
        if not channel:
            logger.error(f"Channel {channel_id} not found")
            return
            
        # Send a message to the channel
        await channel.send("🔄 Testing CSV processor with fixed timestamp format (YYYY.MM.DD-HH.MM.SS)...")
        
        # Get the CSVProcessorCog
        csv_processor = client.get_cog('CSVProcessorCog')
        if not csv_processor:
            await channel.send("❌ Error: CSV processor not found")
            return
            
        # Use the _get_server_configs method to get server configurations
        configs = await csv_processor._get_server_configs()
        if not configs:
            await channel.send("❌ Error: No server configurations found")
            return
            
        # Get the active server config
        config = configs[0]
        server_id = config.get('server_id')
        server_name = config.get('name', 'Unknown')
        
        # Set last processed date to 60 days ago
        csv_processor.last_processed[server_id] = datetime.now(timezone.utc) - timedelta(days=60)
        
        # Send status message
        status_msg = await channel.send(f"⏳ Processing server {server_name} with fixed timestamp format...")
        
        # Run the processor
        result = await csv_processor._process_server_csv_files(config)
        
        # Check result
        if isinstance(result, dict):
            processed_files = result.get('processed_files', 0)
            total_kills = result.get('total_kills', 0)
            csv_files = result.get('csv_files', [])
            
            # Create embed
            embed = discord.Embed(
                title="CSV Processing Results",
                description=f"✅ Successfully processed server {server_name} with fixed timestamp format",
                color=discord.Color.green()
            )
            
            # Add summary field
            embed.add_field(
                name="Summary",
                value=f"• Files processed: {processed_files}\n• Total kills: {total_kills}",
                inline=False
            )
            
            # Add sample files field if available
            if csv_files:
                files_text = "\n".join([f"• {os.path.basename(f)}" for f in csv_files[:5]])
                if len(csv_files) > 5:
                    files_text += f"\n... and {len(csv_files) - 5} more files"
                    
                embed.add_field(
                    name="Sample CSV Files",
                    value=files_text,
                    inline=False
                )
                
            # Add timestamp note
            embed.add_field(
                name="Timestamp Format",
                value="✅ Successfully using YYYY.MM.DD-HH.MM.SS format with proper parsing",
                inline=False
            )
            
            # Add footer and timestamp
            embed.set_footer(text="Tower of Temptation PvP Statistics")
            embed.timestamp = datetime.now()
            
            # Send the embed
            await channel.send(embed=embed)
            
            # Update status message
            await status_msg.edit(content=f"✅ Successfully processed {processed_files} files with {total_kills} kills")
            
        else:
            # Send error message
            await status_msg.edit(content=f"❌ Error: {result}")
            
    except Exception as e:
        logger.error(f"Error: {e}")
        try:
            if 'channel' in locals():
                await channel.send(f"❌ Error: {str(e)}")
        except:
            pass

# Run the async function
if __name__ == "__main__":
    asyncio.run(run_command())