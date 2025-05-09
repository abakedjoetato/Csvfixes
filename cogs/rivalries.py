"""
Rivalries cog for Tower of Temptation PvP Statistics Discord Bot.

This cog provides commands for tracking and managing player rivalries, including:
1. Viewing individual player rivalries
2. Displaying top rivalries on a server
3. Showing closest and most intense rivalries
4. Managing rivalries and viewing recent activity
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Literal

import discord
from discord.ext import commands
from discord import app_commands

from models.rivalry import Rivalry
from models.player_link import PlayerLink
from models.guild import Guild
from models.server import Server
from models.player import Player
from utils.embed_builder import EmbedBuilder
from utils.helpers import paginate_embeds, has_admin_permission, has_mod_permission, confirm
from utils.decorators import premium_tier_required
from utils.async_utils import BackgroundTask
from utils.discord_utils import server_id_autocomplete  # Import standardized autocomplete function

logger = logging.getLogger(__name__)

class RivalriesCog(commands.Cog):
    """Commands for managing rivalries in Tower of Temptation"""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.ctx_menu = app_commands.ContextMenu(
            name="View Rivalries",
            callback=self.context_view_rivalries,
        )
        self.bot.tree.add_command(self.ctx_menu)

    async def cog_unload(self) -> None:
        self.bot.tree.remove_command(self.ctx_menu.name, type=self.ctx_menu.type)

    async def get_player_id_for_server(self, interaction: discord.Interaction, server_id: str) -> Optional[str]:
        """Get player ID for a server based on Discord ID

        Args:
            interaction: Discord interaction
            server_id: Server ID

        Returns:
            str or None: Player ID if found
        """
        # Check if user has a player link
        player_link = await PlayerLink.get_by_discord_id(interaction.user.id)
        if not player_link:
            # No player link found, inform user
            embed = EmbedBuilder.warning(
                title="No Linked Player",
                description="You don't have any linked players. Use `/link player` to link your Discord account to an in-game player."
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return None

        # Check if user has a player on this server
        player_id = player_link.get_player_id_for_server(server_id)
        if not player_id or player_id == "":
            # No player on this server, inform user
            embed = EmbedBuilder.warning(
                title="No Player on Server",
                description=f"You don't have a linked player on the selected server. Use `/link player` to link your Discord account to an in-game player."
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return None

        return player_id

    @premium_tier_required(3)  # Rivalries require premium tier 3+
    async def context_view_rivalries(self, interaction: discord.Interaction, member: discord.Member) -> None:
        """Context menu command to view a user's rivalries

        Args:
            interaction: Discord interaction
            member: Discord member
        """
        await interaction.response.defer(ephemeral=True)
        
        # Check premium tier for guild
        guild = await Guild.get_guild(self.bot.db, interaction.guild_id)
        if not guild or not guild.check_feature_access("rivalries"):
            embed = EmbedBuilder.create_error_embed(
                "Premium Feature",
                "Rivalries are a premium feature (Tier 3+). Please upgrade to access this feature."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Initialize server_id to None
        server_id = None
        
        # Get server ID from guild config
        if not server_id or server_id == "":
            # Get the first server ID from guild configuration
            server = await Server.get_first_for_guild(self.bot.db, guild.id)
            if not server:
                embed = EmbedBuilder.create_error_embed(
                    "No Server Configured",
                    "You don't have any game servers configured for this Discord server. Use `/setup server add` to add a server first."
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return
            server_id = server.server_id

        # Get user's player link
        player_link = await PlayerLink.get_by_discord_id(member.id)
        if not player_link:
            embed = EmbedBuilder.info(
                title="No Rivalries",
                description=f"{member.display_name} doesn't have any linked players or rivalries."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Get player ID for server
        player_id = player_link.get_player_id_for_server(server_id)
        if not player_id or player_id == "":
            embed = EmbedBuilder.info(
                title="No Rivalries",
                description=f"{member.display_name} doesn't have a linked player on this server."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Get player's rivalries
        rivalries = await Rivalry.get_for_player(server_id, player_id)

        if not rivalries or len(rivalries) == 0:
            embed = EmbedBuilder.info(
                title="No Rivalries",
                description=f"{member.display_name} doesn't have any rivalries yet."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Create embeds for each rivalry
        embeds = []
        for rivalry in rivalries:
            stats = rivalry.get_stats_for_player(player_id)

            # Get information from stats
            opponent_name = stats.get("opponent_name", "Unknown")
            kills = stats.get("kills", 0)
            deaths = stats.get("deaths", 0)
            kd_ratio = stats.get("kd_ratio", 0)
            intensity = stats.get("intensity_score", 0)
            is_leading = stats.get("is_leading", False)

            # Create rivalry embed
            embed = EmbedBuilder.rivalry(
                player1_name=member.display_name,
                player2_name=opponent_name,
                player1_kills=kills,
                player2_kills=deaths,
                total_kills=rivalry.total_kills,
                last_kill=rivalry.last_kill,
                last_weapon=rivalry.last_weapon,
                last_location=rivalry.last_location
            )

            embeds.append(embed)

        # If only one rivalry, show it directly
        if len(embeds) == 1:
            await interaction.followup.send(embed=embeds[0], ephemeral=True)
        else:
            # Show paginated embeds for multiple rivalries
            await paginate_embeds(interaction, embeds, ephemeral=True)

    # Group command for rivalries
    rivalry_group = app_commands.Group(name="rivalry", description="View and manage rivalries")

    @rivalry_group.command(name="list")
    @premium_tier_required(3)  # Rivalries require premium tier 3+
    @app_commands.describe(
        server_id="The server ID (default: first available server)",
        limit="Maximum number of rivalries to show (default: 10)"
    )
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    async def _rivalries_list(
        self,
        interaction: discord.Interaction,
        server_id: Optional[str] = None,
        limit: Optional[int] = 10
    ) -> None:
        """List your rivalries

        Args:
            interaction: Discord interaction
            server_id: Server ID (optional)
            limit: Maximum number of rivalries to show (default: 10)
        """
        await interaction.response.defer()

        # Get server ID from guild config if not provided
        if not server_id or server_id == "":
            # Get guild configuration
            guild = await Guild.get_guild(self.bot.db, interaction.guild_id)
            if not guild:
                embed = EmbedBuilder.create_error_embed(
                    "Guild Not Found",
                    "Could not find guild configuration. Please contact an administrator."
                )
                await interaction.followup.send(embed=embed)
                return
                
            # Get the first server ID from guild configuration
            server = await Server.get_first_for_guild(self.bot.db, guild.id)
            if not server:
                embed = EmbedBuilder.create_error_embed(
                    "No Server Configured",
                    "You don't have any game servers configured for this Discord server. Use `/setup server add` to add a server first."
                )
                await interaction.followup.send(embed=embed)
                return
            server_id = server.server_id

        # Get player ID for server
        player_id = await self.get_player_id_for_server(interaction, server_id)
        if not player_id or player_id == "":
            return

        # Get player's rivalries
        rivalries = await Rivalry.get_for_player(server_id, player_id)

        if not rivalries or len(rivalries) == 0:
            embed = EmbedBuilder.info(
                title="No Rivalries",
                description="You don't have any rivalries yet. Play more and engage with other players!"
            )
            await interaction.followup.send(embed=embed)
            return

        # Limit number of rivalries
        rivalries = rivalries[:limit]

        # Create embeds for each rivalry
        embeds = []
        for rivalry in rivalries:
            stats = rivalry.get_stats_for_player(player_id)

            # Get information from stats
            opponent_name = stats.get("opponent_name", "Unknown")
            kills = stats.get("kills", 0)
            deaths = stats.get("deaths", 0)
            kd_ratio = stats.get("kd_ratio", 0)
            intensity = stats.get("intensity_score", 0)
            is_leading = stats.get("is_leading", False)

            # Create rivalry embed
            embed = EmbedBuilder.rivalry(
                player1_name=interaction.user.display_name,
                player2_name=opponent_name,
                player1_kills=kills,
                player2_kills=deaths,
                total_kills=rivalry.total_kills,
                last_kill=rivalry.last_kill,
                last_weapon=rivalry.last_weapon,
                last_location=rivalry.last_location
            )

            embeds.append(embed)

        # If only one rivalry, show it directly
        if len(embeds) == 1:
            await interaction.followup.send(embed=embeds[0])
        else:
            # Show paginated embeds for multiple rivalries
            await paginate_embeds(interaction, embeds)

    @rivalry_group.command(name="player")
    @premium_tier_required(3)  # Rivalries require premium tier 3+
    @app_commands.describe(
        server_id="The server ID (default: first available server)",
        player_name="The player name to view rivalries for"
    )
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    async def _rivalries_player(
        self,
        interaction: discord.Interaction,
        player_name: str,
        server_id: Optional[str] = None
    ) -> None:
        """View rivalries for a specific player

        Args:
            interaction: Discord interaction
            server_id: Server ID (optional)
            player_name: Player name
        """
        await interaction.response.defer()

        # Get server ID from guild config if not provided
        if not server_id or server_id == "":
            # Get guild configuration
            guild = await Guild.get_guild(self.bot.db, interaction.guild_id)
            if not guild:
                embed = EmbedBuilder.create_error_embed(
                    "Guild Not Found",
                    "Could not find guild configuration. Please contact an administrator."
                )
                await interaction.followup.send(embed=embed)
                return
                
            # Get the first server ID from guild configuration
            server = await Server.get_first_for_guild(self.bot.db, guild.id)
            if not server:
                embed = EmbedBuilder.create_error_embed(
                    "No Server Configured",
                    "You don't have any game servers configured for this Discord server. Use `/setup server add` to add a server first."
                )
                await interaction.followup.send(embed=embed)
                return
            server_id = server.server_id

        # TODO: In a real implementation, we would verify the player name
        # For now, we'll just assume player_name is valid and is the player ID
        player_id = player_name

        # Get player's rivalries
        rivalries = await Rivalry.get_for_player(server_id, player_id)

        if not rivalries or len(rivalries) == 0:
            embed = EmbedBuilder.info(
                title="No Rivalries",
                description=f"Player `{player_name}` doesn't have any rivalries yet."
            )
            await interaction.followup.send(embed=embed)
            return

        # Create embeds for each rivalry
        embeds = []
        for rivalry in rivalries:
            try:
                stats = rivalry.get_stats_for_player(player_id)

                # Get information from stats
                opponent_name = stats.get("opponent_name", "Unknown")
                kills = stats.get("kills", 0)
                deaths = stats.get("deaths", 0)
                kd_ratio = stats.get("kd_ratio", 0)
                intensity = stats.get("intensity_score", 0)
                is_leading = stats.get("is_leading", False)

                # Determine which player is which
                if player_id == rivalry.player1_id:
                    # Player is player1
                    embed = EmbedBuilder.rivalry(
                        player1_name=player_name,
                        player2_name=opponent_name,
                        player1_kills=kills,
                        player2_kills=deaths,
                        total_kills=rivalry.total_kills,
                        last_kill=rivalry.last_kill,
                        last_weapon=rivalry.last_weapon,
                        last_location=rivalry.last_location
                    )
                else:
                    # Player is player2
                    embed = EmbedBuilder.rivalry(
                        player1_name=player_name,
                        player2_name=opponent_name,
                        player1_kills=kills,
                        player2_kills=deaths,
                        total_kills=rivalry.total_kills,
                        last_kill=rivalry.last_kill,
                        last_weapon=rivalry.last_weapon,
                        last_location=rivalry.last_location
                    )

                embeds.append(embed)
            except ValueError:
                # Skip rivalries where player isn't part of it
                continue

        if not embeds or len(embeds) == 0:
            embed = EmbedBuilder.info(
                title="No Rivalries",
                description=f"Player `{player_name}` doesn't have any valid rivalries."
            )
            await interaction.followup.send(embed=embed)
            return

        # If only one rivalry, show it directly
        if len(embeds) == 1:
            await interaction.followup.send(embed=embeds[0])
        else:
            # Show paginated embeds for multiple rivalries
            await paginate_embeds(interaction, embeds)

    @rivalry_group.command(name="top")
    @premium_tier_required(3)  # Rivalries require premium tier 3+
    @app_commands.describe(
        server_id="The server ID (default: first available server)",
        limit="Maximum number of rivalries to show (default: 10)"
    )
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    async def _rivalries_top(
        self,
        interaction: discord.Interaction,
        server_id: Optional[str] = None,
        limit: Optional[int] = 10
    ) -> None:
        """View top rivalries by total kills

        Args:
            interaction: Discord interaction
            server_id: Server ID (optional)
            limit: Maximum number of rivalries to show (default: 10)
        """
        await interaction.response.defer()

        # Get server ID from guild config if not provided
        if not server_id or server_id == "":
            # Get guild configuration
            guild = await Guild.get_guild(self.bot.db, interaction.guild_id)
            if not guild:
                embed = EmbedBuilder.create_error_embed(
                    "Guild Not Found",
                    "Could not find guild configuration. Please contact an administrator."
                )
                await interaction.followup.send(embed=embed)
                return
                
            # Get the first server ID from guild configuration
            server = await Server.get_first_for_guild(self.bot.db, guild.id)
            if not server:
                embed = EmbedBuilder.create_error_embed(
                    "No Server Configured",
                    "You don't have any game servers configured for this Discord server. Use `/setup server add` to add a server first."
                )
                await interaction.followup.send(embed=embed)
                return
            server_id = server.server_id

        # Get top rivalries
        rivalries = await Rivalry.get_top_rivalries(server_id, limit)

        if not rivalries or len(rivalries) == 0:
            embed = EmbedBuilder.info(
                title="No Rivalries",
                description=f"There are no rivalries on server `{server_id}` yet."
            )
            await interaction.followup.send(embed=embed)
            return

        # Create embeds for each rivalry
        embeds = []
        for rivalry in rivalries:
            embed = EmbedBuilder.rivalry(
                player1_name=rivalry.player1_name,
                player2_name=rivalry.player2_name,
                player1_kills=rivalry.player1_kills,
                player2_kills=rivalry.player2_kills,
                total_kills=rivalry.total_kills,
                last_kill=rivalry.last_kill,
                last_weapon=rivalry.last_weapon,
                last_location=rivalry.last_location
            )

            embeds.append(embed)

        # If only one rivalry, show it directly
        if len(embeds) == 1:
            await interaction.followup.send(embed=embeds[0])
        else:
            # Show paginated embeds for multiple rivalries
            await paginate_embeds(interaction, embeds)

    @rivalry_group.command(name="closest")
    @premium_tier_required(3)  # Rivalries require premium tier 3+
    @app_commands.describe(
        server_id="The server ID (default: first available server)",
        limit="Maximum number of rivalries to show (default: 10)"
    )
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    async def _rivalries_closest(
        self,
        interaction: discord.Interaction,
        server_id: Optional[str] = None,
        limit: Optional[int] = 10
    ) -> None:
        """View closest rivalries by score difference

        Args:
            interaction: Discord interaction
            server_id: Server ID (optional)
            limit: Maximum number of rivalries to show (default: 10)
        """
        await interaction.response.defer()

        # Get server ID from guild config if not provided
        if not server_id or server_id == "":
            # Get guild configuration
            guild = await Guild.get_guild(self.bot.db, interaction.guild_id)
            if not guild:
                embed = EmbedBuilder.create_error_embed(
                    "Guild Not Found",
                    "Could not find guild configuration. Please contact an administrator."
                )
                await interaction.followup.send(embed=embed)
                return
                
            # Get the first server ID from guild configuration
            server = await Server.get_first_for_guild(self.bot.db, guild.id)
            if not server:
                embed = EmbedBuilder.create_error_embed(
                    "No Server Configured",
                    "You don't have any game servers configured for this Discord server. Use `/setup server add` to add a server first."
                )
                await interaction.followup.send(embed=embed)
                return
            server_id = server.server_id

        # Get closest rivalries
        rivalries = await Rivalry.get_closest_rivalries(server_id, limit)

        if not rivalries or len(rivalries) == 0:
            embed = EmbedBuilder.info(
                title="No Rivalries",
                description=f"There are no rivalries on server `{server_id}` yet."
            )
            await interaction.followup.send(embed=embed)
            return

        # Create embeds for each rivalry
        embeds = []
        for rivalry in rivalries:
            embed = EmbedBuilder.rivalry(
                player1_name=rivalry.player1_name,
                player2_name=rivalry.player2_name,
                player1_kills=rivalry.player1_kills,
                player2_kills=rivalry.player2_kills,
                total_kills=rivalry.total_kills,
                last_kill=rivalry.last_kill,
                last_weapon=rivalry.last_weapon,
                last_location=rivalry.last_location
            )

            embeds.append(embed)

        # If only one rivalry, show it directly
        if len(embeds) == 1:
            await interaction.followup.send(embed=embeds[0])
        else:
            # Show paginated embeds for multiple rivalries
            await paginate_embeds(interaction, embeds)

    @rivalry_group.command(name="recent")
    @premium_tier_required(3)  # Rivalries require premium tier 3+
    @app_commands.describe(
        server_id="The server ID (default: first available server)",
        limit="Maximum number of rivalries to show (default: 10)",
        days="Number of days to look back (default: 7)"
    )
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    async def _rivalries_recent(
        self,
        interaction: discord.Interaction,
        server_id: Optional[str] = None,
        limit: Optional[int] = 10,
        days: Optional[int] = 7
    ) -> None:
        """View recently active rivalries

        Args:
            interaction: Discord interaction
            server_id: Server ID (optional)
            limit: Maximum number of rivalries to show (default: 10)
            days: Number of days to look back (default: 7)
        """
        await interaction.response.defer()

        # Get server ID from guild config if not provided
        if not server_id or server_id == "":
            # Get guild configuration
            guild = await Guild.get_guild(self.bot.db, interaction.guild_id)
            if not guild:
                embed = EmbedBuilder.create_error_embed(
                    "Guild Not Found",
                    "Could not find guild configuration. Please contact an administrator."
                )
                await interaction.followup.send(embed=embed)
                return
                
            # Get the first server ID from guild configuration
            server = await Server.get_first_for_guild(self.bot.db, guild.id)
            if not server:
                embed = EmbedBuilder.create_error_embed(
                    "No Server Configured",
                    "You don't have any game servers configured for this Discord server. Use `/setup server add` to add a server first."
                )
                await interaction.followup.send(embed=embed)
                return
            server_id = server.server_id

        # Get recent rivalries
        rivalries = await Rivalry.get_recent_rivalries(server_id, limit, days)

        if not rivalries or len(rivalries) == 0:
            embed = EmbedBuilder.info(
                title="No Recent Rivalries",
                description=f"There are no rivalries active in the last {days} days on server `{server_id}`."
            )
            await interaction.followup.send(embed=embed)
            return

        # Create embeds for each rivalry
        embeds = []
        for rivalry in rivalries:
            embed = EmbedBuilder.rivalry(
                player1_name=rivalry.player1_name,
                player2_name=rivalry.player2_name,
                player1_kills=rivalry.player1_kills,
                player2_kills=rivalry.player2_kills,
                total_kills=rivalry.total_kills,
                last_kill=rivalry.last_kill,
                last_weapon=rivalry.last_weapon,
                last_location=rivalry.last_location
            )

            embeds.append(embed)

        # If only one rivalry, show it directly
        if len(embeds) == 1:
            await interaction.followup.send(embed=embeds[0])
        else:
            # Show paginated embeds for multiple rivalries
            await paginate_embeds(interaction, embeds)

    @rivalry_group.command(name="between")
    @premium_tier_required(3)  # Rivalries require premium tier 3+
    @app_commands.describe(
        server_id="The server ID (default: first available server)",
        player1="First player name",
        player2="Second player name"
    )
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    async def _rivalries_between(
        self,
        interaction: discord.Interaction,
        server_id: Optional[str] = None,
        player1: str = None,
        player2: str = None
    ) -> None:
        """View rivalry between two specific players

        Args:
            interaction: Discord interaction
            server_id: Server ID (optional)
            player1: First player name
            player2: Second player name
        """
        await interaction.response.defer()

        # Get server ID from guild config if not provided
        if not server_id or server_id == "":
            # Get guild configuration
            guild = await Guild.get_guild(self.bot.db, interaction.guild_id)
            if not guild:
                embed = EmbedBuilder.create_error_embed(
                    "Guild Not Found",
                    "Could not find guild configuration. Please contact an administrator."
                )
                await interaction.followup.send(embed=embed)
                return
                
            # Get the first server ID from guild configuration
            server = await Server.get_first_for_guild(self.bot.db, guild.id)
            if not server:
                embed = EmbedBuilder.create_error_embed(
                    "No Server Configured",
                    "You don't have any game servers configured for this Discord server. Use `/setup server add` to add a server first."
                )
                await interaction.followup.send(embed=embed)
                return
            server_id = server.server_id

        # Validate player names
        if not player1 or not player2 or player1 == "" or player2 == "":
            embed = EmbedBuilder.error(
                title="Missing Players",
                description="Please specify both player names."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # TODO: In a real implementation, we would verify the player names
        # For now, we'll just assume they are valid and are the player IDs
        player1_id = player1
        player2_id = player2

        # Get rivalry between players
        rivalry = await Rivalry.get_by_players(server_id, player1_id, player2_id)

        if not rivalry:
            embed = EmbedBuilder.info(
                title="No Rivalry",
                description=f"There is no rivalry between `{player1}` and `{player2}` on server `{server_id}`."
            )
            await interaction.followup.send(embed=embed)
            return

        # Create rivalry embed
        embed = EmbedBuilder.rivalry(
            player1_name=rivalry.player1_name,
            player2_name=rivalry.player2_name,
            player1_kills=rivalry.player1_kills,
            player2_kills=rivalry.player2_kills,
            total_kills=rivalry.total_kills,
            last_kill=rivalry.last_kill,
            last_weapon=rivalry.last_weapon,
            last_location=rivalry.last_location
        )

        await interaction.followup.send(embed=embed)

    @rivalry_group.command(name="record_kill")
    @premium_tier_required(3)  # Rivalries require premium tier 3+
    @app_commands.describe(
        server_id="The server ID",
        killer="Killer player name",
        victim="Victim player name",
        weapon="Weapon used (optional)",
        location="Kill location (optional)"
    )
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    async def _rivalries_record_kill(
        self,
        interaction: discord.Interaction,
        server_id: str,
        killer: str,
        victim: str,
        weapon: Optional[str] = None,
        location: Optional[str] = None
    ) -> None:
        """Record a kill and update rivalry (Admin only)

        Args:
            interaction: Discord interaction
            server_id: Server ID
            killer: Killer player name
            victim: Victim player name
            weapon: Weapon used (optional)
            location: Kill location (optional)
        """
        await interaction.response.defer(ephemeral=True)

        # Check premium tier for guild
        guild = await Guild.get_guild(self.bot.db, interaction.guild_id)
        if not guild or not guild.check_feature_access("rivalries"):
            embed = EmbedBuilder.create_error_embed(
                "Premium Feature",
                "Rivalries are a premium feature (Tier 3+). Please upgrade to access this feature."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Check admin permissions
        admin_permission = has_admin_permission(interaction)
        if not admin_permission:
            embed = EmbedBuilder.error(
                title="Permission Denied",
                description="You don't have permission to use this command."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # TODO: In a real implementation, we would verify the player names
        # For now, we'll just assume they are valid and are the player IDs
        killer_id = killer
        victim_id = victim

        # Record kill
        rivalry = await Rivalry.record_kill(
            server_id=server_id,
            killer_id=killer_id,
            killer_name=killer,
            victim_id=victim_id,
            victim_name=victim,
            weapon=weapon,
            location=location
        )

        # Create success embed
        embed = EmbedBuilder.success(
            title="Kill Recorded",
            description=f"Successfully recorded kill: `{killer}` killed `{victim}`"
        )

        # Add details
        if weapon:
            embed.add_field(
                name="Weapon",
                value=weapon,
                inline=True
            )

        if location:
            embed.add_field(
                name="Location",
                value=location,
                inline=True
            )

        await interaction.followup.send(embed=embed)

        # Show updated rivalry
        rivalry_embed = EmbedBuilder.rivalry(
            player1_name=rivalry.player1_name,
            player2_name=rivalry.player2_name,
            player1_kills=rivalry.player1_kills,
            player2_kills=rivalry.player2_kills,
            total_kills=rivalry.total_kills,
            last_kill=rivalry.last_kill,
            last_weapon=rivalry.last_weapon,
            last_location=rivalry.last_location
        )

        await interaction.followup.send(embed=rivalry_embed)

async def setup(bot: commands.Bot) -> None:
    """Set up the rivalries cog"""
    await bot.add_cog(RivalriesCog(bot))