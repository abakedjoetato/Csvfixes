"""
Premium features and management commands
"""
import logging
import discord
from discord.ext import commands
from discord import app_commands
from typing import List, Optional

from models.guild import Guild
from utils.embed_builder import EmbedBuilder
from utils.helpers import is_home_guild_admin, has_admin_permission
from utils.decorators import premium_tier_required, requires_home_guild_admin
from config import PREMIUM_TIERS, EMBED_THEMES

logger = logging.getLogger(__name__)

class Premium(commands.Cog):
    """Premium features and management commands"""

    def __init__(self, bot):
        self.bot = bot

    @commands.hybrid_group(name="premium", description="Premium management commands")
    @commands.guild_only()
    async def premium(self, ctx):
        """Premium command group"""
        if ctx.invoked_subcommand is None:
            await ctx.send("Please specify a subcommand.")

    @premium.command(name="tiers", description="View available premium tiers and features")
    async def tiers(self, ctx):
        """View available premium tiers and features"""
        try:
            # Get guild model for themed embed
            guild_data = None
            guild_model = None
            try:
                guild_data = await self.bot.db.guilds.find_one({"guild_id": ctx.guild.id})
                if guild_data:
                    # Use create_from_db_document to ensure proper conversion of premium_tier
                    guild_model = Guild.create_from_db_document(guild_data, self.bot.db)
                    logger.info(f"PREMIUM DEBUG: Premium status - guild premium tier: {guild_model.premium_tier}")
            except Exception as e:
                logger.warning(f"Error getting guild model: {e}")

            # Create base embed
            embed = EmbedBuilder.create_base_embed(
                "Premium Tiers",
                "View available premium tiers and their features",
                guild=guild_model
            )

            # Add info for each tier
            for tier_id, tier_info in PREMIUM_TIERS.items():
                # Get tier info
                tier_name = tier_info.get("name", f"Tier {tier_id}")
                price = tier_info.get("price", "£0")
                if tier_id == 0:
                    tier_title = f"{tier_name} (Free)"
                else:
                    tier_title = f"{tier_name} ({price})"

                # Format features list
                features = tier_info.get("features", [])
                max_servers = tier_info.get("max_servers", 0)

                feature_display = {
                    "killfeed": "Killfeed",
                    "basic_stats": "Basic Statistics",
                    "leaderboards": "Leaderboards",
                    "rivalries": "Rivalries System",
                    "bounties": "Bounty System",
                    "player_links": "Player Connections",
                    "factions": "Factions",
                    "economy": "Economy System",
                    "advanced_analytics": "Advanced Analytics"
                }

                feature_list = [f"✅ {feature_display.get(f, f)}" for f in features]

                # Create feature text
                # Get price information (already retrieved earlier)
                price_text = "Free" if price == 0 else f"£{price}/month"
                
                feature_text = "**Features:**\n" + "\n".join(feature_list)
                feature_text += f"\n\n**Max Servers:** {max_servers}"
                feature_text += f"\n**Price:** {price_text}"

                # Add field for this tier
                embed.add_field(
                    name=tier_title,
                    value=feature_text,
                    inline=False
                )

            # Add footer note
            embed.set_footer(text="Contact the bot owner to upgrade premium tier")

            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error in premium tiers command: {e}")
            embed = await EmbedBuilder.create_error_embed(
                "Error",
                f"Failed to show premium tiers: {e}"
            )
            await ctx.send(embed=embed)

    @premium.command(name="status", description="Check premium status of this guild")
    async def status(self, ctx):
        """Check the premium status of this guild"""
        try:
            # Get guild model for themed embed (using the new get_or_create method)
            guild_model = None
            try:
                # Premium is guild-based, not server-based (Rule #9)
                # Use get_or_create to ensure premium works without requiring server setup
                guild_model = await Guild.get_or_create(self.bot.db, ctx.guild.id, ctx.guild.name)
            except Exception as e:
                logger.warning(f"Error getting guild model: {e}")

            # Premium is guild-based, not server-based (Rule #9)
            # Use get_or_create to ensure premium works without requiring server setup
            guild = await Guild.get_or_create(self.bot.db, ctx.guild.id, ctx.guild.name)
            
            # If we couldn't get or create a guild, that's a serious database error
            if not guild:
                embed = await EmbedBuilder.create_error_embed(
                    "Database Error",
                    "Failed to access guild data. Please try again later."
                , guild=guild_model)
                await ctx.send(embed=embed)
                return

            # Create embed using guild model
            tier_name = PREMIUM_TIERS.get(guild.premium_tier, {}).get('name', f'Tier {guild.premium_tier}')
            price = PREMIUM_TIERS.get(guild.premium_tier, {}).get('price', 0)
            price_text = "Free" if price == 0 else f"£{price}/month"
            
            embed = await EmbedBuilder.create_base_embed(
                f"Premium Status for {ctx.guild.name}",
                f"Current tier: **{tier_name}** ({price_text})", 
                guild=guild)

            # Add tier information
            tier_info = PREMIUM_TIERS.get(guild.premium_tier, {})

            # Server slots
            max_servers = tier_info.get("max_servers", 1)
            current_servers = len(guild.servers)

            embed.add_field(
                name="Server Slots",
                value=f"{current_servers}/{max_servers} used",
                inline=True
            )

            # Features
            features = tier_info.get("features", [])
            feature_display = {
                "killfeed": "Killfeed",
                "basic_stats": "Basic Statistics",
                "leaderboards": "Leaderboards",
                "rivalries": "Rivalries System",
                "bounties": "Bounty System",
                "player_links": "Player Connections",
                "factions": "Factions",
                "economy": "Economy System",
                "advanced_analytics": "Advanced Analytics"
            }

            feature_list = []
            for feature, display_name in feature_display.items():
                if feature in features:
                    feature_list.append(f"✅ {display_name}")
                else:
                    feature_list.append(f"❌ {display_name}")

            embed.add_field(
                name="Features",
                value="\n".join(feature_list),
                inline=False
            )

            # Add upgrade info
            if guild.premium_tier < 4:  # 4 is Overseer tier
                embed.add_field(
                    name="Upgrade",
                    value="To upgrade to a higher tier, please use `/premium upgrade`.",
                    inline=False
                )

            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error checking premium status: {e}", exc_info=True)
            # Get guild model for themed embed if possible - using get_or_create to maintain Rule #9
            try:
                # Premium is guild-based, not server-based (Rule #9)
                # Use get_or_create to ensure premium works without requiring server setup
                guild_model = await Guild.get_or_create(self.bot.db, ctx.guild.id, ctx.guild.name)
                embed = await EmbedBuilder.create_error_embed(
                    "Error",
                    f"An error occurred while checking premium status: {e}",
                    guild=guild_model)
            except:
                embed = await EmbedBuilder.create_error_embed(
                    "Error",
                    f"An error occurred while checking premium status: {e}")
            await ctx.send(embed=embed)

    @premium.command(name="upgrade", description="Request a premium upgrade")
    async def upgrade(self, ctx):
        """Request a premium upgrade"""

        try:
            # Get guild model for themed embed
            guild_model = None
            try:
                # Premium is guild-based, not server-based (Rule #9)
                # Use get_or_create to ensure premium works without requiring server setup
                guild_model = await Guild.get_or_create(self.bot.db, ctx.guild.id, ctx.guild.name)
            except Exception as e:
                logger.warning(f"Error getting guild model: {e}")

            # Premium is guild-based, not server-based (Rule #9)
            # Use get_or_create to ensure premium works without requiring server setup
            guild = await Guild.get_or_create(self.bot.db, ctx.guild.id, ctx.guild.name)
            
            # If we couldn't get or create a guild, that's a serious database error
            if not guild:
                embed = await EmbedBuilder.create_error_embed(
                    "Database Error",
                    "Failed to access guild data. Please try again later."
                , guild=guild_model)
                await ctx.send(embed=embed)
                return

            # Check if already at max tier
            if guild.premium_tier >= 4:  # Overseer is tier 4
                embed = await EmbedBuilder.create_error_embed(
                    "Maximum Tier",
                    "This guild is already at the maximum premium tier (Overseer)."
                , guild=guild_model)
                await ctx.send(embed=embed)
                return

            # Get home guild
            home_guild = self.bot.get_guild(self.bot.home_guild_id)
            if not home_guild:
                embed = await EmbedBuilder.create_error_embed(
                    "Error",
                    "Could not find the home guild. Please contact the bot owner."
                , guild=guild_model)
                await ctx.send(embed=embed)
                return

            # Get admin channel in home guild
            admin_channel = None
            for channel in home_guild.text_channels:
                if channel.name.lower() in ["admin", "bot-admin", "premium-requests"]:
                    admin_channel = channel
                    break

            if not admin_channel:
                embed = await EmbedBuilder.create_error_embed(
                    "Error",
                    "Could not find an admin channel in the home guild. Please contact the bot owner directly."
                , guild=guild_model)
                await ctx.send(embed=embed)
                return

            # Create request embed
            request_embed = discord.Embed(
                title="Premium Upgrade Request",
                description=f"A guild has requested a premium upgrade.",
                color=discord.Color.gold(),
                timestamp=discord.utils.utcnow()
            )

            request_embed.add_field(name="Guild Name", value=ctx.guild.name, inline=True)
            request_embed.add_field(name="Guild ID", value=str(ctx.guild.id), inline=True)
            request_embed.add_field(name="Current Tier", value=str(guild.premium_tier), inline=True)
            request_embed.add_field(name="Requested By", value=f"{ctx.author} ({ctx.author.id})", inline=True)

            # Add server count
            request_embed.add_field(name="Current Servers", value=str(len(guild.servers)), inline=True)

            # Add a way to contact the requester
            request_embed.add_field(
                name="Approve Command",
                value=f"`!admin premium {ctx.guild.id} {guild.premium_tier + 1}`",
                inline=False
            )

            # Send request to admin channel
            await admin_channel.send(embed=request_embed)

            # Send confirmation to user
            embed = await EmbedBuilder.create_success_embed(
                "Request Sent",
                "Your premium upgrade request has been sent to the administrators. "
                "You will be notified once your request has been processed."
            )

            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error requesting premium upgrade: {e}", exc_info=True)
            embed = await EmbedBuilder.create_error_embed(
                "Error",
                f"An error occurred while requesting a premium upgrade: {e}"
            , guild=guild_model)
            await ctx.send(embed=embed)

    @premium.command(name="features", description="View available premium features")
    async def features(self, ctx):
        """View available premium features"""

        try:
            # Get guild model for themed embed
            guild_model = None
            try:
                # Premium is guild-based, not server-based (Rule #9)
                # Use get_or_create to ensure premium works without requiring server setup
                guild_model = await Guild.get_or_create(self.bot.db, ctx.guild.id, ctx.guild.name)
            except Exception as e:
                logger.warning(f"Error getting guild model: {e}")

            # Create embed
            embed = await EmbedBuilder.create_base_embed(
                "Premium Features",
                "Overview of premium features by tier"
            , guild=guild_model)

            # Add tier information
            for tier, info in PREMIUM_TIERS.items():
                # Get tier info
                tier_name = info.get("name", f"Tier {tier}")
                price = info.get("price", "£0")
                features = info.get("features", [])
                max_servers = info.get("max_servers", 0)

                feature_display = {
                    "killfeed": "Killfeed",
                    "basic_stats": "Basic Statistics",
                    "leaderboards": "Leaderboards",
                    "rivalries": "Rivalries System",
                    "bounties": "Bounty System",
                    "player_links": "Player Connections",
                    "factions": "Factions",
                    "economy": "Economy System",
                    "advanced_analytics": "Advanced Analytics"
                }

                feature_list = []
                for feature, display_name in feature_display.items():
                    if feature in features:
                        feature_list.append(f"✅ {display_name}")
                    else:
                        feature_list.append(f"❌ {display_name}")

                # Add tier field
                tier_title = f"{tier_name}" + (f" ({price})" if tier > 0 else " (Free)")
                embed.add_field(
                    name=f"{tier_title} - {max_servers} server{'s' if max_servers != 1 else ''}",
                    value="\n".join(feature_list),
                    inline=False
                )

            # Add upgrade info
            embed.add_field(
                name="How to Upgrade",
                value="Use `/premium upgrade` to request a premium upgrade.",
                inline=False
            )

            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error showing premium features: {e}", exc_info=True)
            embed = await EmbedBuilder.create_error_embed(
                "Error",
                f"An error occurred while showing premium features: {e}"
            , guild=guild_model)
            await ctx.send(embed=embed)

    # The /premium set command has been removed in favor of using only the /admin premium command
    # This ensures premium tier management is only accessible through one command path
    # The /admin premium command already has the same functionality and is restricted to home guild admins


    @premium.command(name="theme", description="Set the theme for embed displays (Warlord+ only)")
    @app_commands.describe(
        theme="The theme to use for embeds"
    )
    @app_commands.choices(theme=[
        app_commands.Choice(name="Default", value="default"),
        app_commands.Choice(name="Midnight", value="midnight"),
        app_commands.Choice(name="Blood", value="blood"),
        app_commands.Choice(name="Gold", value="gold"),
        app_commands.Choice(name="Toxic", value="toxic"),
        app_commands.Choice(name="Ghost", value="ghost")
    ])
    @premium_tier_required(3)  # Custom embeds require Warlord tier (3+)
    async def set_theme(self, ctx, theme: str):
        """Set the theme for embed displays (Warlord+ only)"""
        try:
            # Check if user has admin permission
            if not has_admin_permission(ctx):
                # Get guild model for themed embed
                # Premium is guild-based, not server-based (Rule #9)
                guild_model = await Guild.get_or_create(self.bot.db, ctx.guild.id, ctx.guild.name)
                embed = await EmbedBuilder.create_error_embed(
                    "Permission Denied",
                    "You need administrator permission or the designated admin role to use this command.",
                    guild=guild_model)
                await ctx.send(embed=embed, ephemeral=True)
                return

            # Premium is guild-based, not server-based (Rule #9)
            # Use get_or_create to ensure premium works without requiring server setup
            guild = await Guild.get_or_create(self.bot.db, ctx.guild.id, ctx.guild.name)
            if not guild:
                # This is a database error, not a setup issue
                embed = await EmbedBuilder.create_error_embed(
                    "Database Error",
                    "Failed to access guild data. Please try again later.")
                await ctx.send(embed=embed)
                return

            # Premium tier is now checked by the decorator
            # No need for manual premium check here

            # Check if theme exists
            from config import EMBED_THEMES
            if theme != "default" and theme not in EMBED_THEMES:
                embed = await EmbedBuilder.create_error_embed(
                    "Invalid Theme",
                    f"The theme '{theme}' does not exist.",
                    guild=guild)
                await ctx.send(embed=embed)
                return

            # Set theme
            success = await guild.set_theme(theme)
            if not success:
                embed = await EmbedBuilder.create_error_embed(
                    "Error",
                    "Failed to set theme. Please try again later.",
                    guild=guild)
                await ctx.send(embed=embed)
                return

            # Create preview embed with new theme
            embed = await EmbedBuilder.create_base_embed(
                "Theme Set Successfully",
                f"Your guild's theme has been set to **{EMBED_THEMES[theme]['name']}**. All embeds will now use this theme.",
                guild=guild
            )

            embed.add_field(
                name="Preview",
                value="This is a preview of your new theme.",
                inline=False
            )

            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error setting theme: {e}", exc_info=True)
            # Get guild model for themed embed - using get_or_create to maintain Rule #9
            try:
                # Premium is guild-based, not server-based (Rule #9)
                # Use get_or_create to ensure premium works without requiring server setup
                guild_model = await Guild.get_or_create(self.bot.db, ctx.guild.id, ctx.guild.name)
                embed = await EmbedBuilder.create_error_embed(
                    "Error",
                    f"An error occurred while setting the theme: {e}",
                    guild=guild_model)
            except:
                embed = await EmbedBuilder.create_error_embed(
                    "Error",
                    f"An error occurred while setting the theme: {e}")
            await ctx.send(embed=embed)

    @premium.command(name="testupdate", description="Test premium tier update")
    @commands.is_owner()
    async def testupdate(self, ctx, tier: int):
        """Test updating the premium tier (Bot Owner only)"""
        try:
            # Get guild model
            guild = await Guild.get_or_create(self.bot.db, ctx.guild.id, ctx.guild.name)
            
            if not guild:
                await ctx.send("Failed to get guild model")
                return
                
            # Log before state
            logger.info(f"TEST: Premium tier before update: {guild.premium_tier}")
            
            # Update premium tier
            result = await guild.set_premium_tier(self.bot.db, tier)
            
            # Log after state
            logger.info(f"TEST: Premium tier update result: {result}")
            
            # Re-fetch guild to verify
            updated_guild = await Guild.get_by_guild_id(self.bot.db, str(ctx.guild.id))
            logger.info(f"TEST: Premium tier after update: {updated_guild.premium_tier if updated_guild else 'None'}")
            
            await ctx.send(f"Premium tier update test: Result={result}, Before={guild.premium_tier}, After={updated_guild.premium_tier if updated_guild else 'None'}")
            
        except Exception as e:
            logger.error(f"Error in test update: {e}", exc_info=True)
            await ctx.send(f"Error: {e}")

async def setup(bot):
    """Set up the Premium cog"""
    await bot.add_cog(Premium(bot))