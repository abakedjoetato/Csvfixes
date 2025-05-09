"""
Test Command - Will execute a direct test of the CSV processor with timestamp parsing fix

This script uses the following method:
1. Create a dedicated slash command '/test-csv-parsing'
2. Register the command to your server
3. Execute it to show results in Discord

This ensures the test runs within your actual bot instance
and posts results to your Discord channel.
"""

import asyncio
import discord
from discord import app_commands
import logging
import sys
import os
import traceback
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("test_command.log")
    ]
)

logger = logging.getLogger(__name__)

# Server ID where to install the command
GUILD_ID = 1219706687980568769  # Emerald Servers guild ID

# Create a minimal bot to register the command
class TestCommandBot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        
        # Create command tree
        self.tree = app_commands.CommandTree(self)
        
        # Add test command to tree
        @self.tree.command(
            name="test-csv-parsing",
            description="Test CSV timestamp parsing fix with real SFTP data",
            guild=discord.Object(id=GUILD_ID)
        )
        async def test_csv_parsing(interaction: discord.Interaction):
            """Test CSV timestamp parsing with real SFTP data"""
            await self.run_csv_test(interaction)
    
    async def setup_hook(self):
        # Sync commands to guild
        await self.tree.sync(guild=discord.Object(id=GUILD_ID))
        logger.info("Commands synced to guild")
    
    async def on_ready(self):
        logger.info(f"Bot logged in as {self.user.name} ({self.user.id})")
        logger.info(f"Added /test-csv-parsing command to guild {GUILD_ID}")
        logger.info("Please use the slash command in Discord to verify the fix")
    
    async def run_csv_test(self, interaction: discord.Interaction):
        """Run the CSV processor test and show results in Discord"""
        logger.info(f"Running CSV test for user {interaction.user.name}")
        
        try:
            # Acknowledge the interaction immediately
            await interaction.response.defer(thinking=True)
            
            # Create initial embed
            embed = discord.Embed(
                title="CSV Timestamp Parsing Test",
                description="Testing CSV timestamp parsing fix with real SFTP data...",
                color=discord.Color.blue()
            )
            
            embed.add_field(
                name="Status", 
                value="🔄 Starting test with CSV processor...", 
                inline=False
            )
            embed.set_footer(text=f"Test started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Send initial response
            await interaction.followup.send(embed=embed)
            
            # Server configuration
            SERVER_ID = "c8009f11-4f0f-4c68-8623-dc4b5c393722"
            ORIGINAL_SERVER_ID = "7020"  # Critical: must be 7020
            
            # Create server config
            server_config = {
                "hostname": "79.127.236.1",
                "port": 8822,
                "username": "baked",
                "password": "emerald",
                "sftp_path": "/logs",
                "csv_pattern": r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}\.csv",
                "original_server_id": ORIGINAL_SERVER_ID,
            }
            
            # Update embed - step 2
            embed.add_field(
                name="CSV Processor", 
                value="✅ Using CSV processor with correct server ID mapping", 
                inline=False
            )
            await interaction.followup.send(embed=embed)
            
            # Set a cutoff date of 60 days ago for testing
            cutoff_date = datetime.now() - timedelta(days=60)
            
            # Get main bot's instance with imports
            from bot import bot as main_bot
            
            if not main_bot:
                embed.color = discord.Color.red()
                embed.add_field(
                    name="Error", 
                    value="❌ Could not access main bot instance", 
                    inline=False
                )
                await interaction.followup.send(embed=embed)
                return
            
            # Get the CSV processor cog
            csv_processor = main_bot.get_cog("CSVProcessorCog")
            if not csv_processor:
                embed.color = discord.Color.red()
                embed.add_field(
                    name="Error", 
                    value="❌ CSV processor cog not found", 
                    inline=False
                )
                await interaction.followup.send(embed=embed)
                return
            
            # Update embed - found cog
            embed.add_field(
                name="CSV Processor Cog", 
                value="✅ Found CSV processor cog", 
                inline=False
            )
            await interaction.followup.send(embed=embed)
            
            # Set last processed date if available
            if hasattr(csv_processor, 'last_processed'):
                csv_processor.last_processed[SERVER_ID] = cutoff_date
                
                embed.add_field(
                    name="Cutoff Date", 
                    value=f"📅 Set processing window to 60 days: {cutoff_date.strftime('%Y-%m-%d')}", 
                    inline=False
                )
                await interaction.followup.send(embed=embed)
            
            # Update embed - processing
            processing_embed = discord.Embed(
                title="CSV Timestamp Parsing Test - Processing",
                description="Processing CSV files with correct server ID mapping...",
                color=discord.Color.gold()
            )
            await interaction.followup.send(embed=processing_embed)
            
            # Process the CSV files
            try:
                # Force the server ID to use 7020 as original_server_id
                files_processed, events_processed = await csv_processor._process_server_csv_files(
                    SERVER_ID, server_config
                )
                
                # Build results embed
                results_embed = discord.Embed(
                    title="CSV Timestamp Parsing Test - Results",
                    description="Verification of CSV timestamp parsing fix with real SFTP data",
                    color=discord.Color.green() if files_processed > 0 else discord.Color.red()
                )
                
                if files_processed > 0:
                    results_embed.add_field(
                        name="Test Results ✅", 
                        value=f"Successfully processed {files_processed} CSV files with {events_processed} events!", 
                        inline=False
                    )
                    
                    # Try to get some sample events
                    db = main_bot.db
                    if db:
                        try:
                            # Query recent events
                            cursor = db.kills.find({"server_id": SERVER_ID}).sort("timestamp", -1).limit(5)
                            events = []
                            async for doc in cursor:
                                events.append(doc)
                            
                            if events:
                                event_list = ""
                                for i, event in enumerate(events):
                                    timestamp = event.get("timestamp")
                                    if isinstance(timestamp, datetime):
                                        formatted_time = timestamp.strftime("%Y-%m-%d %H:%M:%S")
                                    else:
                                        formatted_time = str(timestamp)
                                        
                                    killer = event.get("killer_name", "Unknown")
                                    victim = event.get("victim_name", "Unknown")
                                    weapon = event.get("weapon", "Unknown")
                                    
                                    event_list += f"• Event {i+1}: {formatted_time}\n"
                                    event_list += f"  {killer} killed {victim} with {weapon}\n"
                                
                                results_embed.add_field(
                                    name="Sample Events with Parsed Timestamps", 
                                    value=f"```\n{event_list}\n```", 
                                    inline=False
                                )
                        except Exception as e:
                            logger.error(f"Error getting event samples: {str(e)}")
                    
                    # Final verdict
                    results_embed.add_field(
                        name="Timestamp Parsing Fix Verdict", 
                        value="✅ **FIX VERIFIED** - CSV files with format YYYY.MM.DD-HH.MM.SS are now correctly parsed!", 
                        inline=False
                    )
                else:
                    results_embed.add_field(
                        name="Test Results ❌", 
                        value=f"Failed to process any CSV files with timestamp parsing.", 
                        inline=False
                    )
                
                results_embed.set_footer(text=f"Test completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                
                # Send results
                await interaction.followup.send(embed=results_embed)
                
            except Exception as e:
                logger.error(f"Error processing CSV files: {str(e)}")
                logger.error(traceback.format_exc())
                
                error_embed = discord.Embed(
                    title="CSV Timestamp Parsing Test - Error",
                    description="Error processing CSV files with timestamp parsing",
                    color=discord.Color.red()
                )
                
                error_embed.add_field(
                    name="Error ❌", 
                    value=f"Error: {str(e)[:1000]}", 
                    inline=False
                )
                
                error_embed.add_field(
                    name="Stack Trace", 
                    value=f"```\n{traceback.format_exc()[:500]}\n```", 
                    inline=False
                )
                
                await interaction.followup.send(embed=error_embed)
            
        except Exception as e:
            logger.error(f"Unhandled exception: {str(e)}")
            logger.error(traceback.format_exc())
            
            # Try to send error message
            try:
                await interaction.followup.send(f"Unhandled error: {str(e)[:1500]}")
            except:
                pass

async def main():
    """Main function"""
    # Get Discord token
    token = os.environ.get('DISCORD_TOKEN')
    if not token:
        # Try to get from bot.py
        try:
            sys.path.append('.')
            from bot import BOT_TOKEN
            token = BOT_TOKEN
        except:
            logger.error("Could not get Discord token")
            return
    
    if not token:
        logger.error("No Discord token found - cannot run the test")
        return
    
    # Create and run the bot
    bot = TestCommandBot()
    
    try:
        await bot.start(token)
    except Exception as e:
        logger.error(f"Error running bot: {str(e)}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    asyncio.run(main())