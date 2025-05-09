"""
Admin commands for bot management
"""
import os
import logging
import asyncio
import discord
from discord.ext import commands
from discord import app_commands
from typing import Optional, cast, Dict, Any, List, TypeVar, Union, Protocol

from models.guild import Guild
from utils.embed_builder import EmbedBuilder
from utils.helpers import is_home_guild_admin
from utils.decorators import premium_tier_required, has_admin_permission, requires_home_guild_admin

logger = logging.getLogger(__name__)

class Admin(commands.Cog):
    """Admin commands for bot management"""

    def __init__(self, bot):
        self.bot = bot

    @commands.hybrid_group(name="admin", description="Admin commands")
    @commands.guild_only()
    @commands.has_permissions(administrator=True)
    async def admin(self, ctx):
        """Admin command group"""
        if ctx.invoked_subcommand is None:
            await ctx.send("Please specify a subcommand.")

    @admin.command(name="setrole", description="Set the admin role for server management")
    @app_commands.describe(role="The role to set as admin")
    async def setrole(self, ctx, role: discord.Role):
        """Set the admin role for server management"""
        try:
            # Get guild model for themed embed
            guild_model = None
            try:
                guild_model = await Guild.get_by_guild_id(self.bot.db, str(ctx.guild.id))
            except Exception as e:
                logger.warning(f"Error getting guild model: {e}")

            # Get guild data
            guild = await Guild.get_by_guild_id(self.bot.db, str(ctx.guild.id))
            if guild is None:
                # Include db parameter in Guild instantiation
                guild = Guild(
                    self.bot.db,
                    guild_id=str(ctx.guild.id),
                    name=ctx.guild.name
                )
                await self.bot.db.guilds.insert_one(guild.to_dict())

            # Set admin role
            await guild.set_admin_role(self.bot.db, str(role.id))

            # Send success message
            embed = EmbedBuilder.create_success_embed(
                "Admin Role Set",
                f"The {role.mention} role has been set as the admin role for server management."
            , guild=guild_model)
            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error setting admin role: {e}", exc_info=True)
            embed = EmbedBuilder.create_error_embed(
                "Error",
                f"An error occurred while setting the admin role: {e}"
            , guild=guild_model)
            await ctx.send(embed=embed)

    @admin.command(name="premium", description="Set the premium tier for a guild")
    @app_commands.describe(
        guild_id="The ID of the guild to set premium for",
        tier="The premium tier to set (0-4)"
    )
    # Note: This command uses guild_id, not server_id, as it operates on guilds not servers
    @requires_home_guild_admin()
    async def premium(self, ctx, guild_id: str, tier: int):
        """Set the premium tier for a guild (Home Guild Admins only)"""

        try:
            # Get guild model for themed embed
            guild_model = None
            try:
                guild_model = await Guild.get_by_guild_id(self.bot.db, str(ctx.guild.id))
                logger.info(f"Retrieved guild model for current guild: {ctx.guild.id} - premium tier: {guild_model.premium_tier if guild_model is not None else 'None'}")
            except Exception as e:
                logger.warning(f"Error getting guild model: {e}")

            # Validate tier
            if tier < 0 or tier > 4:
                logger.warning(f"Invalid tier {tier} specified for premium command")
                embed = await EmbedBuilder.create_error_embed(
                    "Invalid Tier",
                    "Premium tier must be between 0 and 4 (Scavenger, Survivor, Mercenary, Warlord, Overseer).",
                    guild=guild_model
                )
                await ctx.send(embed=embed)
                return

            try:
                # Get guild data with timeout protection
                async with asyncio.timeout(5.0):  # 5 second timeout
                    logger.info(f"Attempting to retrieve guild with ID: {guild_id}")
                    target_guild = await Guild.get_by_guild_id(self.bot.db, guild_id)
                    
                    if target_guild is None:
                        logger.warning(f"Guild with ID {guild_id} not found in database")
                        
                        # Creating new guild with proper db parameter
                        logger.info(f"Creating new guild with ID: {guild_id}")
                        
                        # Try to get Discord guild name if possible is not None
                        try:
                            discord_guild = self.bot.get_guild(int(guild_id))
                            guild_name = discord_guild.name if discord_guild is not None else f"Guild {guild_id}"
                        except:
                            guild_name = f"Guild {guild_id}"
                        
                        # Create new guild with db parameter
                        target_guild = Guild(
                            self.bot.db,
                            guild_id=str(guild_id),
                            name=guild_name,
                            premium_tier=tier  # Set premium tier immediately
                        )
                        
                        # Save to database
                        await self.bot.db.guilds.insert_one(target_guild.to_dict())
                        logger.info(f"Created new guild in database: {guild_id} with tier {tier}")
                        
                        # Skip the set_premium_tier call below since we already set it
                        premium_already_set = True
                    else:
                        premium_already_set = False
                        logger.info(f"Found existing guild: {guild_id} - Current premium tier: {target_guild.premium_tier}")
                        
                    if target_guild is None:
                        embed = await EmbedBuilder.create_error_embed(
                            "Guild Not Found",
                            f"Could not find a guild with ID {guild_id} and failed to create it.",
                            guild=guild_model
                        )
                        await ctx.send(embed=embed)
                        return
            except asyncio.TimeoutError:
                embed = await EmbedBuilder.create_error_embed(
                    "Database Timeout",
                    "The request took too long to process. Please try again.",
                    guild=guild_model
                )
                await ctx.send(embed=embed)
                return
            except Exception as e:
                logger.error(f"Error in premium command: {e}")
                embed = await EmbedBuilder.create_error_embed(
                    "Error",
                    "An unexpected error occurred. Please try again.",
                    guild=guild_model
                )
                await ctx.send(embed=embed)
                return

            # Set premium tier if we didn't already set it during creation
            # FIXED: The old logic was backward and skipping updates when it shouldn't
            if not premium_already_set:
                logger.info(f"Setting premium tier for existing guild {guild_id} from {target_guild.premium_tier} to {tier}")
                result = await target_guild.set_premium_tier(self.bot.db, tier)
                logger.info(f"Premium tier update result: {result} - Guild {guild_id} new tier: {target_guild.premium_tier}")
            else:
                logger.info(f"Skipping premium tier update for guild {guild_id} as it was set during creation")

            # Get guild name from bot
            try:
                bot_guild = self.bot.get_guild(int(guild_id))
                guild_name = bot_guild.name if bot_guild is not None else target_guild.name or f"Guild {guild_id}"
            except Exception as e:
                logger.warning(f"Error getting guild name: {e}")
                guild_name = target_guild.name or f"Guild {guild_id}"

            # Send success message
            embed = await EmbedBuilder.create_success_embed(
                "Premium Tier Set",
                f"The premium tier for {guild_name} has been set to **Tier {tier}**."
            , guild=guild_model)
            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error setting premium tier: {e}", exc_info=True)
            embed = await EmbedBuilder.create_error_embed(
                "Error",
                f"An error occurred while setting the premium tier: {e}"
            , guild=guild_model)
            await ctx.send(embed=embed)

    @admin.command(name="status", description="View bot status information")
    async def status(self, ctx):
        """View bot status information"""

        try:
            # Get guild model for themed embed
            guild_model = None
            try:
                guild_model = await Guild.get_by_guild_id(self.bot.db, str(ctx.guild.id))
            except Exception as e:
                logger.warning(f"Error getting guild model: {e}")

            # Get basic statistics
            guild_count = len(self.bot.guilds)

            # Count servers and players
            server_count = 0
            player_count = 0
            kill_count = 0

            try:
                # Use model count methods
                server_count = await self.bot.db.game_servers.count_documents({})
                player_count = await self.bot.db.players.count_documents({})
                kill_count = await self.bot.db.kills.count_documents({})
            except Exception as e:
                logger.warning(f"Error counting documents: {e}")

            # Create embed
            embed = await EmbedBuilder.create_base_embed(
                "Bot Status",
                "Current statistics and performance information"
            , guild=guild_model)

            # Add statistics fields
            embed.add_field(name="Guilds", value=str(guild_count), inline=True)
            embed.add_field(name="Servers", value=str(server_count), inline=True)
            embed.add_field(name="Players", value=str(player_count), inline=True)
            embed.add_field(name="Kills Tracked", value=str(kill_count), inline=True)

            # Add uptime if available is not None
            import time
            if hasattr(self.bot, "start_time"):
                uptime = time.time() - self.bot.start_time
                hours, remainder = divmod(uptime, 3600)
                minutes, seconds = divmod(remainder, 60)
                embed.add_field(
                    name="Uptime",
                    value=f"{int(hours)}h {int(minutes)}m {int(seconds)}s",
                    inline=True
                )

            # Add background tasks info
            task_count = len(self.bot.background_tasks) if hasattr(self.bot, "background_tasks") else 0
            embed.add_field(name="Background Tasks", value=str(task_count), inline=True)

            # Send embed
            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error getting status: {e}", exc_info=True)
            embed = await EmbedBuilder.create_error_embed(
                "Error",
                f"An error occurred while getting bot status: {e}"
            , guild=guild_model)
            await ctx.send(embed=embed)

    @admin.command(name="sethomeguild", description="Set the home guild for the bot")
    async def sethomeguild(self, ctx):
        """Set the current guild as the home guild (Bot Owner only)"""

        # First, defer the response to avoid timeouts
        if hasattr(ctx, 'interaction') and ctx.interaction:
            await ctx.interaction.response.defer(ephemeral=False)
            is_deferred = True
        else:
            is_deferred = False

        try:
            # Get guild model for themed embed
            guild_data = None
            guild_model = None
            try:
                guild_data = await self.bot.db.guilds.find_one({"guild_id": ctx.guild.id})
                if guild_data is not None:
                    # Use create_from_db_document to ensure proper conversion of premium_tier
                    guild_model = Guild.create_from_db_document(guild_data, self.bot.db)
            except Exception as e:
                logger.warning(f"Error getting guild model: {e}")

            # Check if user is not None is the bot owner
            if ctx.author.id != self.bot.owner_id:
                embed = await EmbedBuilder.create_error_embed(
                    "Permission Denied",
                    "Only the bot owner can use this command."
                , guild=guild_model)

                if is_deferred is not None and hasattr(ctx.interaction, 'followup'):
                    await ctx.interaction.followup.send(embed=embed, ephemeral=True)
                else:
                    await ctx.send(embed=embed, ephemeral=True)
                return

            # Set home guild
            self.bot.home_guild_id = ctx.guild.id

            # Store in environment variable for current session (for immediate access)
            os.environ["HOME_GUILD_ID"] = str(ctx.guild.id)

            # Store in database for persistence across restarts
            try:
                # Upsert the home guild ID in the bot_config collection
                await self.bot.db.bot_config.update_one(
                    {"key": "home_guild_id"}, 
                    {"$set": {"key": "home_guild_id", "value": str(ctx.guild.id)}},
                    upsert=True
                )
                logger.info(f"Updated database with new home guild ID: {ctx.guild.id}")
                
                # Try to update .env file as a backup (may not work in Replit)
                try:
                    with open(".env", "r") as f:
                        lines = f.readlines()

                    with open(".env", "w") as f:
                        env_updated = False
                        for line in lines:
                            if line.startswith("HOME_GUILD_ID="):
                                f.write(f"HOME_GUILD_ID={str(ctx.guild.id)}\n")
                                env_updated = True
                            else:
                                f.write(line)
                        
                        # Add the line if it doesn't exist
                        if not env_updated:
                            f.write(f"HOME_GUILD_ID={str(ctx.guild.id)}\n")

                    logger.info(f"Updated .env file with new home guild ID: {ctx.guild.id}")
                except Exception as env_error:
                    logger.warning(f"Failed to update .env file (non-critical): {env_error}")
            except Exception as db_error:
                logger.error(f"Failed to update database with home guild ID: {db_error}", exc_info=True)

            # Send success message
            embed = await EmbedBuilder.create_success_embed(
                "Home Guild Set",
                f"This guild ({ctx.guild.name}) has been set as the home guild for the bot."
            , guild=guild_model)

            if is_deferred is not None and hasattr(ctx.interaction, 'followup'):
                await ctx.interaction.followup.send(embed=embed)
            else:
                await ctx.send(embed=embed)

            logger.info(f"Home guild set to {ctx.guild.name} (ID: {ctx.guild.id}) by owner")

        except Exception as e:
            logger.error(f"Error setting home guild: {e}", exc_info=True)
            embed = await EmbedBuilder.create_error_embed(
                "Error",
                f"An error occurred while setting the home guild: {e}"
            , guild=guild_model)

            if is_deferred is not None and hasattr(ctx.interaction, 'followup'):
                await ctx.interaction.followup.send(embed=embed)
            else:
                await ctx.send(embed=embed)



    @admin.command(name="help", description="Show help for admin commands")
    async def admin_help(self, ctx):
        """Show help for admin commands"""
        # Get guild model for themed embed
        guild_data = None
        guild_model = None
        try:
            guild_data = await self.bot.db.guilds.find_one({"guild_id": ctx.guild.id})
            if guild_data is not None:
                # Use create_from_db_document to ensure proper conversion of premium_tier
                    guild_model = Guild.create_from_db_document(guild_data, self.bot.db)
        except Exception as e:
            logger.warning(f"Error getting guild model: {e}")

        embed = await EmbedBuilder.create_base_embed(
            "Admin Commands Help",
            "List of available admin commands and their usage"
        , guild=guild_model)

        # Add command descriptions
        embed.add_field(
            name="`/admin setrole <role>`",
            value="Set the admin role for server management",
            inline=False
        )

        embed.add_field(
            name="`/admin premium <guild_id> <tier>`",
            value="Set the premium tier for a guild (Home Guild Admins only)",
            inline=False
        )

        embed.add_field(
            name="`/admin sethomeguild`",
            value="Set the current guild as the home guild (Bot Owner only)",
            inline=False
        )

        embed.add_field(
            name="`/admin status`",
            value="View bot status information",
            inline=False
        )

        await ctx.send(embed=embed)

async def setup(bot):
    """Set up the Admin cog"""
    await bot.add_cog(Admin(bot))