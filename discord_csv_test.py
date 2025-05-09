"""
Direct Discord Command Test

This script runs a direct test of the CSV processor using the Discord bot commands
to verify that the timestamp parsing fix works in a real-world environment with 
real Discord interactions.
"""

import discord
from discord.ext import commands
import logging
import sys
import asyncio
import os
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('discord_test.log')
    ]
)

logger = logging.getLogger(__name__)

# Channel ID for output
CHANNEL_ID = 1360632422957449237

# Server ID to test
SERVER_ID = "c8009f11-4f0f-4c68-8623-dc4b5c393722"

class TestBot(commands.Bot):
    """Test bot that runs CSV commands"""
    
    def __init__(self):
        super().__init__(command_prefix="!", intents=discord.Intents.all())
        self.testing_task = None
        
    async def on_ready(self):
        """Called when bot is ready"""
        logger.info(f"Bot logged in as {self.user.name} ({self.user.id})")
        
        # Start testing task
        self.testing_task = self.loop.create_task(self.run_tests())
        
    async def run_tests(self):
        """Run the CSV processing tests"""
        try:
            # Sleep to ensure bot is fully ready
            await asyncio.sleep(2)
            
            # Get the target channel
            channel = self.get_channel(CHANNEL_ID)
            if not channel:
                logger.error(f"Channel with ID {CHANNEL_ID} not found")
                await self.close()
                return
                
            logger.info(f"Found target channel: {channel.name}")
                
            # Send initial message
            start_embed = discord.Embed(
                title="CSV Processing Test Started",
                description="Testing CSV timestamp parsing fix with live Discord commands",
                color=discord.Color.blue()
            )
            
            test_message = await channel.send(embed=start_embed)
            
            # Find CSV processor command
            csv_command = None
            
            # Look for the process-csv command
            for command in self.commands:
                if command.name == "process-csv":
                    csv_command = command
                    break
                    
            # If not found, check application commands
            if not csv_command and hasattr(self, 'tree'):
                for command in self.tree.get_commands():
                    if command.name == "process-csv":
                        csv_command = command
                        break
            
            if not csv_command:
                error_embed = discord.Embed(
                    title="Test Failed",
                    description="Could not find the process-csv command",
                    color=discord.Color.red()
                )
                await test_message.edit(embed=error_embed)
                logger.error("Could not find process-csv command")
                await self.close()
                return
                
            logger.info(f"Found command: {csv_command.name}")
            
            # Create status update
            status_embed = discord.Embed(
                title="CSV Processing Test",
                description="Running process-csv command...",
                color=discord.Color.gold()
            )
            await test_message.edit(embed=status_embed)
            
            # Execute the command
            ctx = await self.get_context(test_message)
            
            # Invoke the command with the server ID
            if isinstance(csv_command, discord.app_commands.Command):
                # Slash command
                interaction = None  # We can't create an interaction manually
                logger.info("Cannot test slash command without user interaction")
                
                # Send message to channel asking user to run the command
                user_embed = discord.Embed(
                    title="Manual Testing Required",
                    description=f"Please run the following slash command:\n`/process-csv server_id:{SERVER_ID}`",
                    color=discord.Color.gold()
                )
                user_embed.add_field(
                    name="Why?", 
                    value="Slash commands can only be executed through Discord's UI", 
                    inline=False
                )
                await channel.send(embed=user_embed)
                
                # Wait for the CSV processor to finish
                logger.info("Waiting for user to run the slash command")
                await asyncio.sleep(30)
                
            else:
                # Prefix command
                logger.info(f"Invoking command: !process-csv {SERVER_ID}")
                
                try:
                    await ctx.invoke(csv_command, server_id=SERVER_ID)
                    logger.info("Command invoked successfully")
                except Exception as e:
                    logger.error(f"Error invoking command: {str(e)}")
                    
                    # Try again as a raw message
                    raw_message = await channel.send(f"!process-csv {SERVER_ID}")
                    raw_ctx = await self.get_context(raw_message)
                    
                    try:
                        if raw_ctx.command:
                            await raw_ctx.command.invoke(raw_ctx)
                            logger.info("Command invoked via raw message")
                    except Exception as raw_e:
                        logger.error(f"Error with raw invocation: {str(raw_e)}")
            
            # Final status update
            final_embed = discord.Embed(
                title="CSV Processing Test Completed",
                description="Check the channel for CSV processing results",
                color=discord.Color.green()
            )
            
            final_embed.add_field(
                name="Next Steps",
                value="1. Verify that CSV files were processed\n2. Check for timestamp parsing errors\n3. Confirm events were properly processed",
                inline=False
            )
            
            await test_message.edit(embed=final_embed)
            
            # Add a manual test instruction as well
            manual_embed = discord.Embed(
                title="Manual CSV Processing Test",
                description="If the automatic test didn't work, please run the following command manually:",
                color=discord.Color.blue()
            )
            
            manual_embed.add_field(
                name="Slash Command", 
                value=f"/process-csv server_id:{SERVER_ID}", 
                inline=False
            )
            
            manual_embed.add_field(
                name="Prefix Command",
                value=f"!process-csv {SERVER_ID}",
                inline=False
            )
            
            await channel.send(embed=manual_embed)
            
            # Wait a bit then exit
            await asyncio.sleep(5)
            
        except Exception as e:
            logger.error(f"Error in testing task: {str(e)}")
        finally:
            # Close the bot
            await self.close()

async def main():
    """Main function"""
    # Load the bot token from environment or config
    token = os.environ.get("DISCORD_TOKEN")
    
    if not token:
        try:
            # Try to load from bot.py
            import sys
            sys.path.append('.')
            from bot import BOT_TOKEN
            token = BOT_TOKEN
        except:
            logger.error("Could not get Discord token")
            return
    
    if not token:
        logger.error("Discord token not found")
        return
        
    # Create the test bot
    bot = TestBot()
    
    try:
        # Start the bot
        await bot.start(token)
    except Exception as e:
        logger.error(f"Error starting bot: {str(e)}")
    finally:
        if not bot.is_closed():
            await bot.close()

if __name__ == "__main__":
    asyncio.run(main())