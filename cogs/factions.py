"""
Factions cog for Tower of Temptation PvP Statistics Discord Bot.

This cog provides commands for managing factions, including:
1. Creating and managing factions
2. Joining and leaving factions
3. Managing faction membership and permissions
4. Viewing faction information and statistics
"""
import asyncio
import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Literal

import discord
from discord.ext import commands
from discord import app_commands

from models.faction import Faction, FACTION_ROLES
from utils.embed_builder import EmbedBuilder
from utils.helpers import paginate_embeds, has_admin_permission, has_mod_permission, confirm
from utils.async_utils import BackgroundTask
from utils.premium import premium_tier_required
from utils.discord_utils import server_id_autocomplete  # Import standardized autocomplete function

logger = logging.getLogger(__name__)

class FactionsCog(commands.Cog):
    """Commands for managing factions in Tower of Temptation"""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.ctx_menu = app_commands.ContextMenu(
            name="View Faction",
            callback=self.context_view_faction,
        )
        self.bot.tree.add_command(self.ctx_menu)
        self.server_autocomplete_cache = {}

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
        from models.player_link import PlayerLink

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
        if not player_id:
            # No player on this server, inform user
            embed = EmbedBuilder.warning(
                title="No Player on Server",
                description=f"You don't have a linked player on the selected server. Use `/link player` to link your Discord account to an in-game player."
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return None

        return player_id

    async def context_view_faction(self, interaction: discord.Interaction, member: discord.Member) -> None:
        """Context menu command to view a user's faction

        Args:
            interaction: Discord interaction
            member: Discord member
        """
        await interaction.response.defer(ephemeral=True)

        # Get server ID from guild config
        # For now, hardcode a test server ID
        server_id = "test_server"

        # Get user's player link
        from models.player_link import PlayerLink

        player_link = await PlayerLink.get_by_discord_id(member.id)
        if not player_link:
            embed = EmbedBuilder.info(
                title="No Faction",
                description=f"{member.display_name} doesn't have any linked players or factions."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Get player ID for server
        player_id = player_link.get_player_id_for_server(server_id)
        if not player_id:
            embed = EmbedBuilder.info(
                title="No Faction",
                description=f"{member.display_name} doesn't have a linked player on this server."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Get player's factions
        factions = await Faction.get_for_player(server_id, player_id)

        if not factions or len(factions) == 0:
            embed = EmbedBuilder.info(
                title="No Faction",
                description=f"{member.display_name} isn't a member of any faction."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Show faction info for the first faction
        faction = factions[0]
        faction_members = await faction.get_members()

        # Find member's role
        member_role = "Member"
        for faction_member in faction_members:
            if faction_member.get("player_id") == player_id:
                member_role = faction_member.get("role", "").title()
                break

        # Create embed
        embed = faction.get_discord_embed(interaction.guild)
        embed.add_field(
            name="Role",
            value=member_role,
            inline=True
        )

        await interaction.followup.send(embed=embed, ephemeral=True)

    # Group command for factions
    faction_group = app_commands.Group(name="faction", description="Manage factions")

    @faction_group.command(name="create")
    @app_commands.describe(
        server_id="The server ID (default: first available server)",
        name="The faction name",
        tag="The faction tag (3-10 characters)",
        description="The faction description",
        color="The faction color (hex code or color name)"
    )
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    @premium_tier_required(2)  # Factions require premium tier 2+
    async def _faction_create(
        self,
        interaction: discord.Interaction,
        server_id: Optional[str] = None,
        name: str = None,
        tag: str = None,
        description: Optional[str] = None,
        color: Optional[str] = None
    ) -> None:
        """Create a new faction

        Args:
            interaction: Discord interaction
            server_id: Server ID (optional)
            name: Faction name
            tag: Faction tag
            description: Faction description (optional)
            color: Faction color (optional)
        """
        await interaction.response.defer(ephemeral=True)

        # Get server ID from guild config if not provided
        if not server_id or server_id == "":
            # For now, hardcode a test server ID
            server_id = "test_server"

        # Get player ID for server
        player_id = await self.get_player_id_for_server(interaction, server_id)
        if not player_id:
            return

        # Validate faction name and tag
        if not name or len(name) < 3 or len(name) > 32:
            embed = EmbedBuilder.error(
                title="Invalid Faction Name",
                description="Faction name must be between 3 and 32 characters long."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        if not tag or len(tag) < 2 or len(tag) > 10:
            embed = EmbedBuilder.error(
                title="Invalid Faction Tag",
                description="Faction tag must be between 2 and 10 characters long."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Parse color
        faction_color = None
        if color:
            if color.startswith("#"):
                try:
                    faction_color = int(color[1:], 16)
                except ValueError:
                    embed = EmbedBuilder.error(
                        title="Invalid Color",
                        description="Invalid hex color code. Use format #RRGGBB."
                    )
                    await interaction.followup.send(embed=embed, ephemeral=True)
                    return
            elif color.lower() in EmbedBuilder.FACTION_COLORS:
                faction_color = EmbedBuilder.FACTION_COLORS[color.lower()]
            else:
                embed = EmbedBuilder.error(
                    title="Invalid Color",
                    description="Invalid color name. Use a hex code (#RRGGBB) or one of the following: red, blue, green, gold, purple, orange, teal, dark_blue."
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return

        # Check if player is already in a faction
        existing_factions = await Faction.get_for_player(server_id, player_id)
        if existing_factions and len(existing_factions) > 0:
            embed = EmbedBuilder.error(
                title="Already in Faction",
                description=f"You are already a member of {existing_factions[0].name}. Leave your current faction before creating a new one."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Create faction
        try:
            faction = await Faction.create(
                server_id=server_id,
                name=name,
                tag=tag,
                leader_id=player_id,
                description=description,
                color=faction_color
            )

            # Create success embed
            embed = EmbedBuilder.success(
                title="Faction Created",
                description=f"You have successfully created the faction **{name}** [{tag}]."
            )

            # Add faction details
            if description:
                embed.add_field(
                    name="Description",
                    value=description,
                    inline=False
                )

            embed.add_field(
                name="Members",
                value="1",
                inline=True
            )

            await interaction.followup.send(embed=embed, ephemeral=False)

        except ValueError as e:
            embed = EmbedBuilder.error(
                title="Error Creating Faction",
                description=str(e)
            )
            await interaction.followup.send(embed=embed, ephemeral=True)

    @faction_group.command(name="info")
    @app_commands.describe(
        server_id="The server ID (default: first available server)",
        name="The faction name or tag"
    )
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    @premium_tier_required(2)  # Factions require premium tier 2+
    async def _faction_info(
        self,
        interaction: discord.Interaction,
        server_id: Optional[str] = None,
        name: str = None
    ) -> None:
        """Get information about a faction

        Args:
            interaction: Discord interaction
            server_id: Server ID (optional)
            name: Faction name or tag
        """
        await interaction.response.defer()

        # Get server ID from guild config if not provided
        if not server_id or server_id == "":
            # For now, hardcode a test server ID
            server_id = "test_server"

        # Get faction by name or tag
        if not name:
            embed = EmbedBuilder.error(
                title="Missing Faction Name",
                description="Please provide a faction name or tag."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Try to find faction by name or tag
        faction = await Faction.get_by_name(server_id, name)
        if not faction:
            faction = await Faction.get_by_tag(server_id, name)

        if not faction:
            embed = EmbedBuilder.error(
                title="Faction Not Found",
                description=f"No faction found with name or tag '{name}'."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Get faction members
        faction_members = await faction.get_members()

        # Create embed
        embed = faction.get_discord_embed(interaction.guild)

        # Add leader info
        leader_name = "Unknown"
        for member in faction_members:
            if member.get("role") == "leader":
                leader_name = member.get("player_name", "Unknown")
                break

        embed.add_field(
            name="Leader",
            value=leader_name,
            inline=True
        )

        # Add member breakdown
        role_counts = {"leader": 0, "officer": 0, "member": 0}
        for member in faction_members:
            role = member.get("role", "member")
            role_counts[role] = role_counts.get(role, 0) + 1

        member_breakdown = f"👑 Leader: {role_counts['leader']}\n"
        member_breakdown += f"🛡️ Officers: {role_counts['officer']}\n"
        member_breakdown += f"👥 Members: {role_counts['member']}"

        embed.add_field(
            name="Membership",
            value=member_breakdown,
            inline=True
        )

        await interaction.followup.send(embed=embed)

    @faction_group.command(name="list")
    @app_commands.describe(
        server_id="The server ID (default: first available server)"
    )
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    @premium_tier_required(2)  # Factions require premium tier 2+
    async def _faction_list(
        self,
        interaction: discord.Interaction,
        server_id: Optional[str] = None
    ) -> None:
        """List all factions on a server

        Args:
            interaction: Discord interaction
            server_id: Server ID (optional)
        """
        await interaction.response.defer()

        # Get server ID from guild config if not provided
        if not server_id or server_id == "":
            # For now, hardcode a test server ID
            server_id = "test_server"

        # Get all factions
        factions = await Faction.get_all(server_id)

        if not factions or len(factions) == 0:
            embed = EmbedBuilder.info(
                title="No Factions",
                description="There are no factions on this server yet. Use `/faction create` to create one."
            )
            await interaction.followup.send(embed=embed)
            return

        # Create list of faction embeds
        embeds = []
        for faction in factions:
            embed = faction.get_discord_embed(interaction.guild)
            embeds.append(embed)

        # Send paginated embeds
        await paginate_embeds(interaction, embeds)

    @faction_group.command(name="join")
    @app_commands.describe(
        server_id="The server ID (default: first available server)",
        name="The faction name or tag"
    )
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    @premium_tier_required(2)  # Factions require premium tier 2+
    async def _faction_join(
        self,
        interaction: discord.Interaction,
        server_id: Optional[str] = None,
        name: str = None
    ) -> None:
        """Join a faction

        Args:
            interaction: Discord interaction
            server_id: Server ID (optional)
            name: Faction name or tag
        """
        await interaction.response.defer(ephemeral=True)

        # Get server ID from guild config if not provided
        if not server_id or server_id == "":
            # For now, hardcode a test server ID
            server_id = "test_server"

        # Get player ID for server
        player_id = await self.get_player_id_for_server(interaction, server_id)
        if not player_id:
            return

        # Get faction by name or tag
        if not name:
            embed = EmbedBuilder.error(
                title="Missing Faction Name",
                description="Please provide a faction name or tag."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Try to find faction by name or tag
        faction = await Faction.get_by_name(server_id, name)
        if not faction:
            faction = await Faction.get_by_tag(server_id, name)

        if not faction:
            embed = EmbedBuilder.error(
                title="Faction Not Found",
                description=f"No faction found with name or tag '{name}'."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Check if player is already in a faction
        existing_factions = await Faction.get_for_player(server_id, player_id)
        if existing_factions:
            embed = EmbedBuilder.error(
                title="Already in Faction",
                description=f"You are already a member of {existing_factions[0].name}. Leave your current faction before joining a new one."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Check if faction requires approval
        if faction.require_approval:
            embed = EmbedBuilder.warning(
                title="Approval Required",
                description=f"This faction requires approval to join. Please contact the faction leader."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Check if faction is full
        if faction.member_count >= 100:
            embed = EmbedBuilder.error(
                title="Faction Full",
                description=f"This faction is full and cannot accept more members."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Join faction
        try:
            await faction.add_member(player_id)

            # Create success embed
            embed = EmbedBuilder.success(
                title="Faction Joined",
                description=f"You have successfully joined **{faction.name}** [{faction.tag}]."
            )

            await interaction.followup.send(embed=embed)

        except ValueError as e:
            embed = EmbedBuilder.error(
                title="Error Joining Faction",
                description=str(e)
            )
            await interaction.followup.send(embed=embed, ephemeral=True)

    @faction_group.command(name="leave")
    @app_commands.describe(
        server_id="The server ID (default: first available server)"
    )
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    @premium_tier_required(2)  # Factions require premium tier 2+
    async def _faction_leave(
        self,
        interaction: discord.Interaction,
        server_id: Optional[str] = None
    ) -> None:
        """Leave your current faction

        Args:
            interaction: Discord interaction
            server_id: Server ID (optional)
        """
        await interaction.response.defer(ephemeral=True)

        # Get server ID from guild config if not provided
        if not server_id or server_id == "":
            # For now, hardcode a test server ID
            server_id = "test_server"

        # Get player ID for server
        player_id = await self.get_player_id_for_server(interaction, server_id)
        if not player_id:
            return

        # Get player's factions
        factions = await Faction.get_for_player(server_id, player_id)

        if not factions or len(factions) == 0:
            embed = EmbedBuilder.error(
                title="No Faction",
                description="You are not a member of any faction."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Get the first faction
        faction = factions[0]

        # Check if player is the faction leader
        if faction.leader_id == player_id:
            embed = EmbedBuilder.error(
                title="Faction Leader",
                description="You are the leader of this faction. Transfer leadership with `/faction promote` before leaving."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Ask for confirmation
        confirmed = await confirm(
            interaction,
            f"Are you sure you want to leave **{faction.name}** [{faction.tag}]?",
            ephemeral=True
        )

        if not confirmed:
            embed = EmbedBuilder.info(
                title="Action Cancelled",
                description="You have cancelled leaving the faction."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Leave faction
        try:
            await faction.remove_member(player_id)

            # Create success embed
            embed = EmbedBuilder.success(
                title="Faction Left",
                description=f"You have successfully left **{faction.name}** [{faction.tag}]."
            )

            await interaction.followup.send(embed=embed)

        except ValueError as e:
            embed = EmbedBuilder.error(
                title="Error Leaving Faction",
                description=str(e)
            )
            await interaction.followup.send(embed=embed, ephemeral=True)

    @faction_group.command(name="add")
    @app_commands.describe(
        server_id="The server ID (default: first available server)",
        faction_name="The faction name or tag",
        player_name="The player name to add"
    )
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    @app_commands.checks.has_permissions(manage_guild=True)
    @premium_tier_required(2)  # Factions require premium tier 2+
    async def _faction_add(
        self,
        interaction: discord.Interaction,
        server_id: Optional[str] = None,
        faction_name: str = None,
        player_name: str = None
    ) -> None:
        """Add a player to a faction (Admin only)

        Args:
            interaction: Discord interaction
            server_id: Server ID (optional)
            faction_name: Faction name or tag
            player_name: Player name
        """
        await interaction.response.defer(ephemeral=True)

        # Check admin permissions
        if not has_admin_permission(interaction):
            embed = EmbedBuilder.error(
                title="Permission Denied",
                description="You don't have permission to use this command."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Get server ID from guild config if not provided
        if not server_id or server_id == "":
            # For now, hardcode a test server ID
            server_id = "test_server"

        # Get faction by name or tag
        if not faction_name:
            embed = EmbedBuilder.error(
                title="Missing Faction Name",
                description="Please provide a faction name or tag."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Try to find faction by name or tag
        faction = await Faction.get_by_name(server_id, faction_name)
        if not faction:
            faction = await Faction.get_by_tag(server_id, faction_name)

        if not faction:
            embed = EmbedBuilder.error(
                title="Faction Not Found",
                description=f"No faction found with name or tag '{faction_name}'."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Validate player name
        if not player_name:
            embed = EmbedBuilder.error(
                title="Missing Player Name",
                description="Please provide a player name."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Get player ID
        # This would normally involve a database lookup
        # For now, assume player_name is the player ID
        player_id = player_name

        # Add player to faction
        try:
            await faction.add_member(player_id)

            # Create success embed
            embed = EmbedBuilder.success(
                title="Player Added",
                description=f"Successfully added **{player_name}** to **{faction.name}** [{faction.tag}]."
            )

            await interaction.followup.send(embed=embed)

        except ValueError as e:
            embed = EmbedBuilder.error(
                title="Error Adding Player",
                description=str(e)
            )
            await interaction.followup.send(embed=embed, ephemeral=True)

    @faction_group.command(name="remove")
    @app_commands.describe(
        server_id="The server ID (default: first available server)",
        faction_name="The faction name or tag",
        player_name="The player name to remove"
    )
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    @app_commands.checks.has_permissions(manage_guild=True)
    @premium_tier_required(2)  # Factions require premium tier 2+
    async def _faction_remove(
        self,
        interaction: discord.Interaction,
        server_id: Optional[str] = None,
        faction_name: str = None,
        player_name: str = None
    ) -> None:
        """Remove a player from a faction (Admin only)

        Args:
            interaction: Discord interaction
            server_id: Server ID (optional)
            faction_name: Faction name or tag
            player_name: Player name
        """
        await interaction.response.defer(ephemeral=True)

        # Check admin permissions
        if not has_admin_permission(interaction):
            embed = EmbedBuilder.error(
                title="Permission Denied",
                description="You don't have permission to use this command."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Get server ID from guild config if not provided
        if not server_id or server_id == "":
            # For now, hardcode a test server ID
            server_id = "test_server"

        # Get faction by name or tag
        if not faction_name:
            embed = EmbedBuilder.error(
                title="Missing Faction Name",
                description="Please provide a faction name or tag."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Try to find faction by name or tag
        faction = await Faction.get_by_name(server_id, faction_name)
        if not faction:
            faction = await Faction.get_by_tag(server_id, faction_name)

        if not faction:
            embed = EmbedBuilder.error(
                title="Faction Not Found",
                description=f"No faction found with name or tag '{faction_name}'."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Validate player name
        if not player_name:
            embed = EmbedBuilder.error(
                title="Missing Player Name",
                description="Please provide a player name."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Get player ID
        # This would normally involve a database lookup
        # For now, assume player_name is the player ID
        player_id = player_name

        # Check if player is the faction leader
        if faction.leader_id == player_id:
            embed = EmbedBuilder.error(
                title="Cannot Remove Leader",
                description="Cannot remove the faction leader. Transfer leadership first."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Remove player from faction
        try:
            await faction.remove_member(player_id)

            # Create success embed
            embed = EmbedBuilder.success(
                title="Player Removed",
                description=f"Successfully removed **{player_name}** from **{faction.name}** [{faction.tag}]."
            )

            await interaction.followup.send(embed=embed)

        except ValueError as e:
            embed = EmbedBuilder.error(
                title="Error Removing Player",
                description=str(e)
            )
            await interaction.followup.send(embed=embed, ephemeral=True)

    @faction_group.command(name="promote")
    @app_commands.describe(
        server_id="The server ID (default: first available server)",
        faction_name="The faction name or tag",
        player_name="The player name to promote",
        role="The role to promote to (member, officer, leader)"
    )
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    @premium_tier_required(2)  # Factions require premium tier 2+
    async def _faction_promote(
        self,
        interaction: discord.Interaction,
        server_id: Optional[str] = None,
        faction_name: str = None,
        player_name: str = None,
        role: Literal["member", "officer", "leader"] = "officer"
    ) -> None:
        """Promote a player in your faction

        Args:
            interaction: Discord interaction
            server_id: Server ID (optional)
            faction_name: Faction name or tag
            player_name: Player name
            role: Role to promote to
        """
        await interaction.response.defer(ephemeral=True)

        # Get server ID from guild config if not provided
        if not server_id or server_id == "":
            # For now, hardcode a test server ID
            server_id = "test_server"

        # Get player ID for server
        player_id = await self.get_player_id_for_server(interaction, server_id)
        if not player_id:
            return

        # Get faction by name or tag
        if not faction_name:
            # Try to get player's faction
            factions = await Faction.get_for_player(server_id, player_id)
            if not factions or len(factions) == 0:
                embed = EmbedBuilder.error(
                    title="No Faction",
                    description="You are not a member of any faction."
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return

            faction = factions[0]
        else:
            # Try to find faction by name or tag
            faction = await Faction.get_by_name(server_id, faction_name)
            if not faction:
                faction = await Faction.get_by_tag(server_id, faction_name)

            if not faction:
                embed = EmbedBuilder.error(
                    title="Faction Not Found",
                    description=f"No faction found with name or tag '{faction_name}'."
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return

        # Check if player is the faction leader
        if faction.leader_id != player_id and not has_admin_permission(interaction):
            embed = EmbedBuilder.error(
                title="Permission Denied",
                description="Only the faction leader can promote members."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Validate player name
        if not player_name:
            embed = EmbedBuilder.error(
                title="Missing Player Name",
                description="Please provide a player name."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Get player ID
        # This would normally involve a database lookup
        # For now, assume player_name is the player ID
        target_player_id = player_name

        # Check if target player is in the faction
        faction_members = await faction.get_members()
        target_in_faction = False
        for member in faction_members:
            if member.get("player_id") == target_player_id:
                target_in_faction = True
                break

        if not target_in_faction:
            embed = EmbedBuilder.error(
                title="Player Not in Faction",
                description=f"**{player_name}** is not a member of **{faction.name}**."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Validate role
        if role not in FACTION_ROLES:
            embed = EmbedBuilder.error(
                title="Invalid Role",
                description=f"Invalid role '{role}'. Must be one of: {', '.join(FACTION_ROLES)}."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # If promoting to leader, ensure confirmation
        if role == "leader":
            confirmed = await confirm(
                interaction,
                f"Are you sure you want to transfer leadership of **{faction.name}** to **{player_name}**? You will be demoted to officer.",
                ephemeral=True
            )

            if not confirmed:
                embed = EmbedBuilder.info(
                    title="Action Cancelled",
                    description="Leadership transfer cancelled."
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return

        # Update member role
        try:
            await faction.update_member_role(target_player_id, role)

            # Create success embed
            action = "Transferred leadership to" if role == "leader" else f"Promoted to {role}"
            embed = EmbedBuilder.success(
                title="Member Promoted",
                description=f"Successfully {action} **{player_name}** in **{faction.name}** [{faction.tag}]."
            )

            await interaction.followup.send(embed=embed)

        except ValueError as e:
            embed = EmbedBuilder.error(
                title="Error Promoting Member",
                description=str(e)
            )
            await interaction.followup.send(embed=embed, ephemeral=True)

    @faction_group.command(name="edit")
    @app_commands.describe(
        server_id="The server ID (default: first available server)",
        faction_name="The faction name or tag",
        new_name="The new faction name",
        new_tag="The new faction tag",
        description="The new faction description",
        color="The new faction color (hex code or color name)"
    )
    @premium_tier_required(2)  # Factions require premium tier 2+
    async def _faction_edit(
        self,
        interaction: discord.Interaction,
        server_id: Optional[str] = None,
        faction_name: str = None,
        new_name: Optional[str] = None,
        new_tag: Optional[str] = None,
        description: Optional[str] = None,
        color: Optional[str] = None
    ) -> None:
        """Edit faction details

        Args:
            interaction: Discord interaction
            server_id: Server ID (optional)
            faction_name: Faction name or tag
            new_name: New faction name (optional)
            new_tag: New faction tag (optional)
            description: New faction description (optional)
            color: New faction color (optional)
        """
        await interaction.response.defer(ephemeral=True)

        # Get server ID from guild config if not provided
        if not server_id or server_id == "":
            # For now, hardcode a test server ID
            server_id = "test_server"

        # Get player ID for server
        player_id = await self.get_player_id_for_server(interaction, server_id)
        if not player_id:
            return

        # Get faction by name or tag
        if not faction_name:
            # Try to get player's faction
            factions = await Faction.get_for_player(server_id, player_id)
            if not factions or len(factions) == 0:
                embed = EmbedBuilder.error(
                    title="No Faction",
                    description="You are not a member of any faction."
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return

            faction = factions[0]
        else:
            # Try to find faction by name or tag
            faction = await Faction.get_by_name(server_id, faction_name)
            if not faction:
                faction = await Faction.get_by_tag(server_id, faction_name)

            if not faction:
                embed = EmbedBuilder.error(
                    title="Faction Not Found",
                    description=f"No faction found with name or tag '{faction_name}'."
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return

        # Check if player is the faction leader
        if faction.leader_id != player_id and not has_admin_permission(interaction):
            embed = EmbedBuilder.error(
                title="Permission Denied",
                description="Only the faction leader can edit faction details."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Validate new name and tag
        if new_name and (len(new_name) < 3 or len(new_name) > 32):
            embed = EmbedBuilder.error(
                title="Invalid Faction Name",
                description="Faction name must be between 3 and 32 characters long."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        if new_tag and (len(new_tag) < 2 or len(new_tag) > 10):
            embed = EmbedBuilder.error(
                title="Invalid Faction Tag",
                description="Faction tag must be between 2 and 10 characters long."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Parse color
        faction_color = None
        if color:
            if color.startswith("#"):
                try:
                    faction_color = int(color[1:], 16)
                except ValueError:
                    embed = EmbedBuilder.error(
                        title="Invalid Color",
                        description="Invalid hex color code. Use format #RRGGBB."
                    )
                    await interaction.followup.send(embed=embed, ephemeral=True)
                    return
            elif color.lower() in EmbedBuilder.FACTION_COLORS:
                faction_color = EmbedBuilder.FACTION_COLORS[color.lower()]
            else:
                embed = EmbedBuilder.error(
                    title="Invalid Color",
                    description="Invalid color name. Use a hex code (#RRGGBB) or one of the following: red, blue, green, gold, purple, orange, teal, dark_blue."
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return

        # Prepare update data
        update_data = {}
        if new_name:
            update_data["name"] = new_name
        if new_tag:
            update_data["tag"] = new_tag
        if description:  # Allow empty descriptions
            update_data["description"] = description
        if faction_color:
            update_data["color"] = faction_color

        if not update_data:
            embed = EmbedBuilder.error(
                title="No Changes",
                description="No changes specified."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Update faction
        try:
            await faction.update(update_data)

            # Create success embed
            embed = EmbedBuilder.success(
                title="Faction Updated",
                description=f"Successfully updated **{faction.name}** [{faction.tag}]."
            )

            # Add update details
            for key, value in update_data.items():
                if key == "color":
                    embed.add_field(
                        name="Color",
                        value=f"Updated to #{value:06X}",
                        inline=True
                    )
                else:
                    embed.add_field(
                        name=key.title(),
                        value=str(value),
                        inline=True
                    )

            await interaction.followup.send(embed=embed)

        except ValueError as e:
            embed = EmbedBuilder.error(
                title="Error Updating Faction",
                description=str(e)
            )
            await interaction.followup.send(embed=embed, ephemeral=True)

    @faction_group.command(name="stats")
    @app_commands.describe(
        server_id="The server ID (default: first available server)",
        faction_name="The faction name or tag"
    )
    @premium_tier_required(2)  # Factions require premium tier 2+
    async def _faction_stats(
        self,
        interaction: discord.Interaction,
        server_id: Optional[str] = None,
        faction_name: Optional[str] = None
    ) -> None:
        """View faction statistics

        Args:
            interaction: Discord interaction
            server_id: Server ID (optional)
            faction_name: Faction name or tag (optional - defaults to your faction)
        """
        await interaction.response.defer()

        # Get server ID from guild config if not provided
        if not server_id or server_id == "":
            # For now, hardcode a test server ID
            server_id = "test_server"

        # Get faction by name or tag if provided
        if faction_name:
            faction = await Faction.get_by_name(server_id, faction_name)
            if not faction:
                faction = await Faction.get_by_tag(server_id, faction_name)

            if not faction:
                embed = EmbedBuilder.error(
                    title="Faction Not Found",
                    description=f"No faction found with name or tag '{faction_name}'."
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return
        else:
            # Try to get player's faction
            player_id = await self.get_player_id_for_server(interaction, server_id)
            if not player_id:
                return

            factions = await Faction.get_for_player(server_id, player_id)
            if not factions or len(factions) == 0:
                embed = EmbedBuilder.error(
                    title="No Faction",
                    description="You are not a member of any faction. Specify a faction name to view stats."
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return

            faction = factions[0]

        # Get faction stats
        # Just use the stats from the faction object for now
        stats = faction.stats

        # Create stats embed
        embed = EmbedBuilder.faction(
            faction_name=faction.name,
            faction_tag=faction.tag,
            description=faction.description,
            color=faction.color,
            icon_url=faction.icon_url,
            banner_url=faction.banner_url,
            member_count=faction.member_count,
            stats=stats
        )

        # Add KD ratio
        kills = stats.get("kills", 0)
        deaths = stats.get("deaths", 0)
        if kills or deaths:
            kd_ratio = kills / max(deaths, 1)
            embed.add_field(
                name="K/D Ratio",
                value=f"{kd_ratio:.2f}",
                inline=True
            )

        await interaction.followup.send(embed=embed)

    @faction_group.command(name="delete")
    @app_commands.describe(
        server_id="The server ID (default: first available server)",
        faction_name="The faction name or tag"
    )
    @premium_tier_required(2)  # Factions require premium tier 2+
    async def _faction_delete(
        self,
        interaction: discord.Interaction,
        server_id: Optional[str] = None,
        faction_name: str = None
    ) -> None:
        """Delete a faction (Admin or faction leader only)

        Args:
            interaction: Discord interaction
            server_id: Server ID (optional)
            faction_name: Faction name or tag
        """
        await interaction.response.defer(ephemeral=True)

        # Get server ID from guild config if not provided
        if not server_id or server_id == "":
            # For now, hardcode a test server ID
            server_id = "test_server"

        # Get player ID for server
        player_id = await self.get_player_id_for_server(interaction, server_id)
        if not player_id:
            return

        # Get faction by name or tag
        if not faction_name:
            # Try to get player's faction
            factions = await Faction.get_for_player(server_id, player_id)
            if not factions or len(factions) == 0:
                embed = EmbedBuilder.error(
                    title="No Faction",
                    description="You are not a member of any faction."
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return

            faction = factions[0]
        else:
            # Try to find faction by name or tag
            faction = await Faction.get_by_name(server_id, faction_name)
            if not faction:
                faction = await Faction.get_by_tag(server_id, faction_name)

            if not faction:
                embed = EmbedBuilder.error(
                    title="Faction Not Found",
                    description=f"No faction found with name or tag '{faction_name}'."
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return

        # Check if player is the faction leader or admin
        is_leader = faction.leader_id == player_id
        is_admin = has_admin_permission(interaction)

        if not is_leader and not is_admin:
            embed = EmbedBuilder.error(
                title="Permission Denied",
                description="Only the faction leader or server administrators can delete a faction."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Ask for confirmation
        confirmed = await confirm(
            interaction,
            f"⚠️ **WARNING** ⚠️\n\nAre you sure you want to delete **{faction.name}** [{faction.tag}]?\n\nThis action is **PERMANENT** and cannot be undone. All members will be removed from the faction.",
            ephemeral=True
        )

        if not confirmed:
            embed = EmbedBuilder.info(
                title="Action Cancelled",
                description="Faction deletion cancelled."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Delete faction
        try:
            await faction.delete()

            # Create success embed
            embed = EmbedBuilder.success(
                title="Faction Deleted",
                description=f"Successfully deleted **{faction.name}** [{faction.tag}]."
            )

            await interaction.followup.send(embed=embed)

        except ValueError as e:
            embed = EmbedBuilder.error(
                title="Error Deleting Faction",
                description=str(e)
            )
            await interaction.followup.send(embed=embed, ephemeral=True)

    @faction_group.command(name="set_require_approval")
    @app_commands.describe(
        server_id="The server ID (default: first available server)",
        faction_name="The faction name or tag",
        require_approval="Whether the faction requires approval to join (true/false)"
    )
    @premium_tier_required(2)  # Factions require premium tier 2+
    async def _faction_set_require_approval(
        self,
        interaction: discord.Interaction,
        server_id: Optional[str] = None,
        faction_name: str = None,
        require_approval: bool = False
    ) -> None:
        """Set whether the faction requires approval to join (Admin or faction leader only)

        Args:
            interaction: Discord interaction
            server_id: Server ID (optional)
            faction_name: Faction name or tag
            require_approval: Whether the faction requires approval to join
        """
        await interaction.response.defer(ephemeral=True)

        # Get server ID from guild config if not provided
        if not server_id or server_id == "":
            # For now, hardcode a test server ID
            server_id = "test_server"

        # Get player ID for server
        player_id = await self.get_player_id_for_server(interaction, server_id)
        if not player_id:
            return

        # Get faction by name or tag
        if not faction_name:
            # Try to get player's faction
            factions = await Faction.get_for_player(server_id, player_id)
            if not factions or len(factions) == 0:
                embed = EmbedBuilder.error(
                    title="No Faction",
                    description="You are not a member of any faction."
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return

            faction = factions[0]
        else:
            # Try to find faction by name or tag
            faction = await Faction.get_by_name(server_id, faction_name)
            if not faction:
                faction = await Faction.get_by_tag(server_id, faction_name)

            if not faction:
                embed = EmbedBuilder.error(
                    title="Faction Not Found",
                    description=f"No faction found with name or tag '{faction_name}'."
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return

        # Check if player is the faction leader or admin
        is_leader = faction.leader_id == player_id
        is_admin = has_admin_permission(interaction)

        if not is_leader and not is_admin:
            embed = EmbedBuilder.error(
                title="Permission Denied",
                description="Only the faction leader or server administrators can set faction approval requirements."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Update faction require_approval
        try:
            await faction.update({"require_approval": require_approval})

            # Create success embed
            embed = EmbedBuilder.success(
                title="Faction Updated",
                description=f"Successfully updated approval requirements for **{faction.name}** [{faction.tag}].  Require Approval: {require_approval}"
            )

            await interaction.followup.send(embed=embed)

        except ValueError as e:
            embed = EmbedBuilder.error(
                title="Error Updating Faction",
                description=str(e)
            )
            await interaction.followup.send(embed=embed, ephemeral=True)


async def setup(bot: commands.Bot) -> None:
    """Set up the factions cog"""
    await bot.add_cog(FactionsCog(bot))