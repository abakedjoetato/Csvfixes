"""
Process CSV command cog

This cog adds a /process_csv command that processes CSV files with the fixed timestamp format
and posts results to the specified channel.
"""
import discord
from discord.ext import commands
from discord import app_commands
import logging
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

class ProcessCSVCommandCog(commands.Cog):
    """Cog with command to process CSV files with fixed timestamp format"""
    
    def __init__(self, bot):
        """Initialize cog with bot instance"""
        self.bot = bot
        self.processing_lock = asyncio.Lock()
    
    @app_commands.command(
        name="process_csv",
        description="Process CSV files with fixed timestamp format"
    )
    @app_commands.describe(
        days="Number of days to look back (default: 60)",
        target_channel="Channel to post results (default: current channel)"
    )
    async def process_csv(
        self, 
        interaction: discord.Interaction, 
        days: int = 60, 
        target_channel: Optional[discord.TextChannel] = None
    ):
        """Process CSV files with fixed timestamp format
        
        Args:
            interaction: Discord interaction
            days: Number of days to look back
            target_channel: Channel to post results (defaults to current channel)
        """
        # Defer the response since this will take a while
        await interaction.response.defer(ephemeral=False, thinking=True)
        
        # Get the CSV processor cog
        csv_processor = self.bot.get_cog("CSVProcessorCog")
        if not csv_processor:
            await interaction.followup.send("❌ Error: CSVProcessorCog not found")
            return
        
        # Set the target channel
        channel = target_channel or interaction.channel
        
        # Create a detailed embed for the initial response
        embed = discord.Embed(
            title="CSV Processing with Fixed Timestamp Format",
            description="Starting CSV processing with YYYY.MM.DD-HH.MM.SS format support",
            color=discord.Color.blue()
        )
        embed.set_footer(text=f"Requested by {interaction.user}", icon_url=interaction.user.avatar.url if interaction.user.avatar else None)
        embed.timestamp = datetime.now()
        
        # Send the initial response
        response = await interaction.followup.send(embed=embed)
        
        async with self.processing_lock:
            try:
                # Get server configs
                server_configs = await csv_processor._get_server_configs()
                if not server_configs:
                    embed.description = "❌ No server configurations found"
                    embed.color = discord.Color.red()
                    await response.edit(embed=embed)
                    return
                
                # Add server info to embed
                server_names = [config.get("name", "Unknown") for config in server_configs]
                embed.add_field(
                    name="Servers",
                    value=f"Processing {len(server_configs)} servers:\n" + "\n".join([f"• {name}" for name in server_names[:5]]) +
                          (f"\n... and {len(server_names) - 5} more" if len(server_names) > 5 else ""),
                    inline=False
                )
                await response.edit(embed=embed)
                
                # Process each server
                total_processed_files = 0
                total_kills = 0
                
                for i, config in enumerate(server_configs):
                    server_id = config.get("server_id")
                    server_name = config.get("name", "Unknown")
                    
                    if not server_id:
                        continue
                    
                    # Update status in the embed
                    embed.description = f"⏳ Processing server {i+1}/{len(server_configs)}: {server_name}"
                    await response.edit(embed=embed)
                    
                    # Set last processed date to the specified days ago
                    csv_processor.last_processed[server_id] = datetime.now(timezone.utc) - timedelta(days=days)
                    
                    # Process this server's CSV files
                    try:
                        result = await csv_processor._process_server_csv_files(config)
                        
                        # Check the result
                        if isinstance(result, dict):
                            processed_files = result.get("processed_files", 0)
                            server_kills = result.get("total_kills", 0)
                            
                            # Track totals
                            total_processed_files += processed_files
                            total_kills += server_kills
                            
                            # Create server result field
                            embed.add_field(
                                name=f"Server: {server_name}",
                                value=f"✅ Processed {processed_files} files with {server_kills} kills",
                                inline=True
                            )
                            
                            # Post periodic updates
                            if i % 3 == 0 or i == len(server_configs) - 1:
                                await response.edit(embed=embed)
                        else:
                            # Add error field
                            embed.add_field(
                                name=f"Server: {server_name}",
                                value=f"❌ Error: {result}",
                                inline=True
                            )
                            await response.edit(embed=embed)
                            
                    except Exception as e:
                        logger.error(f"Error processing server {server_name}: {e}")
                        # Add error field
                        embed.add_field(
                            name=f"Server: {server_name}",
                            value=f"❌ Error: {str(e)}",
                            inline=True
                        )
                        await response.edit(embed=embed)
                
                # Update final status
                embed.description = f"✅ CSV Processing Complete - Fixed Timestamp Format"
                embed.color = discord.Color.green()
                
                # Add summary field
                embed.add_field(
                    name="Summary",
                    value=f"• Total files processed: {total_processed_files}\n• Total kills processed: {total_kills}\n• Servers processed: {len(server_configs)}",
                    inline=False
                )
                
                # Add timestamp format info
                embed.add_field(
                    name="Timestamp Format",
                    value="✅ Successfully using YYYY.MM.DD-HH.MM.SS format with proper parsing\nExample: 2025.05.03-00.00.00",
                    inline=False
                )
                
                # Send final update
                await response.edit(embed=embed)
                
                # If target channel is different from the interaction channel, copy the result there
                if target_channel and target_channel.id != interaction.channel_id:
                    await target_channel.send(embed=embed)
                
            except Exception as e:
                logger.error(f"Error in process_csv command: {e}")
                # Update embed with error
                embed.description = f"❌ Error in CSV processing: {str(e)}"
                embed.color = discord.Color.red()
                await response.edit(embed=embed)

async def setup(bot):
    """Add the cog to the bot"""
    await bot.add_cog(ProcessCSVCommandCog(bot))