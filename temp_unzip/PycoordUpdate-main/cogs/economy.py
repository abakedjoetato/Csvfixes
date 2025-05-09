"""
Economy commands and gambling features

This module handles all economy-related commands including:
1. Balance checking and management
2. Daily rewards
3. Credits transactions
4. Gambling games (blackjack, slots)
5. Economy statistics
"""
import logging
import discord
from discord.ext import commands
from discord import app_commands
from typing import Optional, List, Dict, Any, Union, cast
import random
import asyncio
import traceback
from datetime import datetime, timedelta
from utils.decorators import command_handler, has_admin_permission

from models.economy import Economy as EconomyModel
from models.guild import Guild
from utils.embed_builder import EmbedBuilder
from utils.gambling import BlackjackGame, BlackjackView, SlotsView
from utils.server_utils import standardize_server_id, validate_server_id_format
from utils.premium import validate_premium_feature, premium_tier_required
from utils.async_utils import AsyncCache, retryable
from utils.discord_utils import server_id_autocomplete  # Import standardized autocomplete function

logger = logging.getLogger(__name__)

class Economy(commands.Cog):
    """Economy commands and gambling features"""

    def __init__(self, bot):
        self.bot = bot
        self.server_autocomplete_cache = {}
        self.active_games = {}

    @commands.hybrid_group(name="economy", description="Economy commands")
    @commands.guild_only()
    async def economy(self, ctx):
        """Economy command group"""
        if not ctx.invoked_subcommand:
            await ctx.send("Please specify a subcommand.")

    @economy.command(name="balance", description="Check your balance")
    @app_commands.describe(server_id="Select a server by name to check balance for")
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    @premium_tier_required(2)  # Economy requires premium tier 2+
    async def balance(self, ctx, server_id: str):
        """Check your balance"""
        # Get guild and server data
        guild = await Guild.get_guild(self.bot.db, ctx.guild.id)
        server = await guild.get_server(server_id)
        server_name = server.get("server_name", server_id)

        # Get player data
        player_id = str(ctx.author.id)
        economy = await EconomyModel.get_by_player(self.bot.db, player_id, server_id)

        if not economy:
            # Create new economy account
            economy = await EconomyModel.create_or_update(self.bot.db, player_id, server_id)

        # Get player balance
        balance = await economy.get_balance()
        lifetime = economy.lifetime_earnings

        # Create embed
        embed = discord.Embed(
            title="💰 Your Balance",
            description=f"Server: {server_name}",
            color=discord.Color.gold()
        )

        embed.add_field(name="Balance", value=f"{balance} credits", inline=True)
        embed.add_field(name="Lifetime Earnings", value=f"{lifetime} credits", inline=True)

        # Get gambling stats with retryable to handle potential timeouts
        gambling_stats = await retryable(
            economy.get_gambling_stats,
            exceptions=(TimeoutError, ConnectionError, Exception),
            max_attempts=2,
            timeout_seconds=5
        )
        
        if gambling_stats:
            blackjack_stats = gambling_stats.get("blackjack", {})
            slots_stats = gambling_stats.get("slots", {})

            blackjack_wins = blackjack_stats.get("wins", 0)
            blackjack_losses = blackjack_stats.get("losses", 0)
            blackjack_earnings = blackjack_stats.get("earnings", 0)

            slots_wins = slots_stats.get("wins", 0)
            slots_losses = slots_stats.get("losses", 0)
            slots_earnings = slots_stats.get("earnings", 0)

            # Add gambling stats to embed
            if blackjack_wins > 0 or blackjack_losses > 0:
                embed.add_field(
                    name="Blackjack Stats",
                    value=f"Wins: {blackjack_wins}, Losses: {blackjack_losses}\nNet Earnings: {blackjack_earnings} credits",
                    inline=False
                )

            if slots_wins > 0 or slots_losses > 0:
                embed.add_field(
                    name="Slots Stats",
                    value=f"Wins: {slots_wins}, Losses: {slots_losses}\nNet Earnings: {slots_earnings} credits",
                    inline=False
                )

        embed.set_footer(text=f"User ID: {player_id}")
        return await ctx.send(embed=embed)

    @economy.command(name="daily", description="Claim your daily reward")
    @app_commands.describe(server_id="Select a server by name to claim daily reward for")
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    @premium_tier_required(2)  # Economy requires premium tier 2+
    async def daily(self, ctx, server_id: str):
        """Claim your daily reward"""
        # Get guild and server data
        guild = await Guild.get_guild(self.bot.db, ctx.guild.id)
        server = await guild.get_server(server_id)
        server_name = server.get("server_name", server_id)

        # Get player data
        player_id = str(ctx.author.id)
        economy = await EconomyModel.get_by_player(self.bot.db, player_id, server_id)

        if not economy:
            # Create new economy account
            economy = await EconomyModel.create_or_update(self.bot.db, player_id, server_id)

        # Calculate daily reward based on premium tier
        daily_amount = 100
        premium_tier = guild.premium_tier
        if premium_tier >= 2:
            daily_amount = 150
        if premium_tier >= 3:
            daily_amount = 200

        # Claim daily reward with retryable to handle potential timeouts
        success, message = await retryable(
            economy.claim_daily,
            args=(daily_amount,),
            exceptions=(TimeoutError, ConnectionError, Exception),
            max_attempts=2,
            timeout_seconds=5
        )

        if success:
            embed = await EmbedBuilder.create_success_embed(
                "💰 Daily Reward",
                message,
                guild=guild
            )
        else:
            embed = await EmbedBuilder.create_error_embed(
                "❌ Daily Reward",
                message,
                guild=guild
            )

        return await ctx.send(embed=embed)

    @economy.command(name="leaderboard", description="View the richest players")
    @app_commands.describe(server_id="Select a server by name to check leaderboard for")
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    @premium_tier_required(2)  # Economy requires premium tier 2+
    async def leaderboard(self, ctx, server_id: str):
        """View the richest players on a server"""
        # Get guild and server data
        guild = await Guild.get_guild(self.bot.db, ctx.guild.id)
        server = await guild.get_server(server_id)
        server_name = server.get("server_name", server_id)
        
        # Get richest players with retry for potential timeouts
        richest_players = await retryable(
            EconomyModel.get_richest_players,
            args=(self.bot.db, server_id, 10),
            exceptions=(TimeoutError, ConnectionError, Exception),
            max_attempts=2,
            timeout_seconds=5
        )
        
        if not richest_players or len(richest_players) == 0:
            embed = await EmbedBuilder.create_error_embed(
                "No Data",
                f"No player economy data found for server {server_name}.",
                guild=guild
            )
            return await ctx.send(embed=embed)

        # Create embed with leaderboard icon
        embed = await EmbedBuilder.create_base_embed(
            title="Richest Players",
            description=f"Server: {server_name}",
            guild=guild
        )

        # Add leaderboard entries
        leaderboard_str = ""
        for i, player in enumerate(richest_players):
            # Use numbers instead of emoji medals
            position = f"#{i+1}"
            player_name = player.get("player_name", "Unknown Player")
            currency = player.get("currency", 0)
            lifetime = player.get("lifetime_earnings", 0)

            leaderboard_str += f"{position} **{player_name}**: {currency} credits (Lifetime: {lifetime})\n"

        embed.add_field(name="Rankings", value=leaderboard_str, inline=False)

        # Get the icon for leaderboard and send with icon
        from utils.embed_icons import send_embed_with_icon, LEADERBOARD_ICON
        return await send_embed_with_icon(ctx, embed, LEADERBOARD_ICON)

    @commands.hybrid_group(name="gambling", description="Gambling commands")
    @commands.guild_only()
    async def gambling(self, ctx):
        """Gambling command group"""
        if not ctx.invoked_subcommand:
            await ctx.send("Please specify a subcommand.")

    @gambling.command(name="blackjack", description="Play blackjack")
    @app_commands.describe(
        server_id="Select a server by name to play on",
        bet="The amount to bet (default: 10)"
    )
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    @premium_tier_required(2)  # Gambling requires premium tier 2+
    async def blackjack(self, ctx, server_id: str, bet: int = 10):
        """Play blackjack"""
        # Get guild and server data
        guild = await Guild.get_guild(self.bot.db, ctx.guild.id)
        server = await guild.get_server(server_id)
        server_name = server.get("server_name", server_id)

        # Validate bet
        if bet <= 0:
            embed = await EmbedBuilder.create_error_embed(
                "Invalid Bet",
                "Bet must be greater than 0.",
                guild=guild
            )
            return await ctx.send(embed=embed)

        # Get player data
        player_id = str(ctx.author.id)
        economy = await EconomyModel.get_by_player(self.bot.db, player_id, server_id)

        if not economy:
            # Create new economy account
            economy = await EconomyModel.create_or_update(self.bot.db, player_id, server_id)

        # Check if player has enough credits
        balance = await economy.get_balance()
        if balance < bet:
            embed = await EmbedBuilder.create_error_embed(
                "Insufficient Funds",
                f"You don't have enough credits. You need {bet} credits to play.",
                guild=guild
            )
            return await ctx.send(embed=embed)

        # Remove the bet amount with retryable to handle potential timeouts
        await retryable(
            economy.remove_currency,
            args=(bet, "blackjack_bet"),
            exceptions=(TimeoutError, ConnectionError, Exception),
            max_attempts=2,
            timeout_seconds=5
        )

        # Start blackjack game
        game = BlackjackGame(player_id)
        game_state = game.start_game(bet)

        # Create embed
        from utils.gambling import create_blackjack_embed
        embed = create_blackjack_embed(game_state)

        # Check for natural blackjack
        if game_state["game_over"]:
            payout = game.get_payout()

            # Update player economy with retryable to handle potential timeouts
            if payout > 0:
                await retryable(
                    economy.add_currency,
                    args=(payout, "blackjack", {"game": "blackjack", "result": game.result}),
                    exceptions=(TimeoutError, ConnectionError, Exception),
                    max_attempts=2,
                    timeout_seconds=5
                )
                await retryable(
                    economy.update_gambling_stats,
                    args=("blackjack", True, payout),
                    exceptions=(TimeoutError, ConnectionError, Exception),
                    max_attempts=2,
                    timeout_seconds=5
                )
                embed.add_field(name="Payout", value=f"You won {payout} credits!", inline=False)
            elif payout < 0:
                await retryable(
                    economy.update_gambling_stats,
                    args=("blackjack", False, abs(payout)),
                    exceptions=(TimeoutError, ConnectionError, Exception),
                    max_attempts=2,
                    timeout_seconds=5
                )
                embed.add_field(name="Loss", value=f"You lost {abs(payout)} credits.", inline=False)
            else:  # push
                embed.add_field(name="Push", value=f"Your bet of {bet} credits has been returned.", inline=False)

            new_balance = await retryable(
                economy.get_balance,
                exceptions=(TimeoutError, ConnectionError, Exception),
                max_attempts=2,
                timeout_seconds=5
            )
            embed.add_field(name="New Balance", value=f"{new_balance} credits", inline=False)

            return await ctx.send(embed=embed)
        else:
            # Create view with buttons
            view = BlackjackView(game, economy)
            message = await ctx.send(embed=embed, view=view)

            # Store the game data
            game.message = message
            game_key = f"{ctx.guild.id}_{player_id}_blackjack"
            self.active_games[game_key] = game
            
            # Return None to indicate success to the command handler
            return None

    @gambling.command(name="slots", description="Play slots")
    @app_commands.describe(
        server_id="Select a server by name to play on",
        bet="The amount to bet (default: 10)"
    )
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    @premium_tier_required(2)  # Gambling requires premium tier 2+
    async def slots(self, ctx, server_id: str, bet: int = 10):
        """Play slots"""
        # Get guild and server data
        guild = await Guild.get_guild(self.bot.db, ctx.guild.id)
        server = await guild.get_server(server_id)
        server_name = server.get("server_name", server_id)
        
        # Validate bet
        if bet <= 0:
            embed = await EmbedBuilder.create_error_embed(
                "Invalid Bet",
                "Bet must be greater than 0.",
                guild=guild
            )
            return await ctx.send(embed=embed)

        # Get player data
        player_id = str(ctx.author.id)
        economy = await EconomyModel.get_by_player(self.bot.db, player_id, server_id)

        if not economy:
            # Create new economy account
            economy = await EconomyModel.create_or_update(self.bot.db, player_id, server_id)

        # Check if player has enough credits
        balance = await retryable(
            economy.get_balance,
            exceptions=(TimeoutError, ConnectionError, Exception),
            max_attempts=2,
            timeout_seconds=5
        )
        
        if balance < bet:
            embed = await EmbedBuilder.create_error_embed(
                "Insufficient Funds",
                f"You don't have enough credits. You need {bet} credits to play.",
                guild=guild
            )
            return await ctx.send(embed=embed)

        # Create slots view with built-in error handling and retries
        view = SlotsView(player_id, economy, bet)
        view.add_error_handler(ctx, guild)  # Pass context and guild for error handling

        # Create initial embed with theme
        embed = await EmbedBuilder.create_base_embed(
            title="Slot Machine",
            description=f"Ready to play! Bet: {bet} credits",
            guild=guild
        )

        embed.add_field(name="Instructions", value="Click 'Spin' to start playing", inline=False)
        embed.add_field(name="Your Balance", value=f"{balance} credits", inline=False)

        # Send with gambling icon
        from utils.embed_icons import send_embed_with_icon, GAMBLING_ICON
        message = await send_embed_with_icon(ctx, embed, GAMBLING_ICON, view=view)
        
        # Store the game data
        view.message = message  # Store the message for updating
        game_key = f"{ctx.guild.id}_{player_id}_slots"
        self.active_games[game_key] = view
        
        # Return None to indicate success to the command handler
        return None

    @economy.command(name="give", description="Give credits to another player")
    @app_commands.describe(
        server_id="Select a server by name",
        user="The user to give credits to",
        amount="The amount to give"
    )
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    @premium_tier_required(2)  # Economy requires premium tier 2+
    async def give(self, ctx, server_id: str, user: discord.Member, amount: int):
        """Give credits to another player"""
        # Get guild and server data
        guild = await Guild.get_guild(self.bot.db, ctx.guild.id)
        server = await guild.get_server(server_id)
        server_name = server.get("server_name", server_id)

        # Validate amount
        if amount <= 0:
            embed = await EmbedBuilder.create_error_embed(
                "Invalid Amount",
                "Amount must be greater than 0.",
                guild=guild
            )
            return await ctx.send(embed=embed)

        # Check if giving to self
        if ctx.author.id == user.id:
            embed = await EmbedBuilder.create_error_embed(
                "Invalid Recipient",
                "You can't give credits to yourself.",
                guild=guild
            )
            return await ctx.send(embed=embed)

        # Get player data with retryable for network resilience
        player_id = str(ctx.author.id)
        player_economy = await retryable(
            EconomyModel.get_by_player,
            args=(self.bot.db, player_id, server_id),
            exceptions=(TimeoutError, ConnectionError, Exception),
            max_attempts=2,
            timeout_seconds=5
        )

        if not player_economy:
            # Create new economy account
            player_economy = await retryable(
                EconomyModel.create_or_update,
                args=(self.bot.db, player_id, server_id),
                exceptions=(TimeoutError, ConnectionError, Exception),
                max_attempts=2,
                timeout_seconds=5
            )

        # Check if player has enough credits
        balance = await retryable(
            player_economy.get_balance,
            exceptions=(TimeoutError, ConnectionError, Exception),
            max_attempts=2,
            timeout_seconds=5
        )
        
        if balance < amount:
            embed = await EmbedBuilder.create_error_embed(
                "Insufficient Funds",
                f"You don't have enough credits. You have {balance} credits.",
                guild=guild
            )
            return await ctx.send(embed=embed)

        # Get recipient data with retryable
        recipient_id = str(user.id)
        recipient_economy = await retryable(
            EconomyModel.get_by_player,
            args=(self.bot.db, recipient_id, server_id),
            exceptions=(TimeoutError, ConnectionError, Exception),
            max_attempts=2,
            timeout_seconds=5
        )

        if not recipient_economy:
            # Create new economy account for recipient
            recipient_economy = await retryable(
                EconomyModel.create_or_update,
                args=(self.bot.db, recipient_id, server_id),
                exceptions=(TimeoutError, ConnectionError, Exception),
                max_attempts=2,
                timeout_seconds=5
            )

        # Transfer credits with retryable for resilience
        await retryable(
            player_economy.remove_currency,
            args=(amount, "transfer", {"recipient_id": recipient_id, "recipient_name": user.name}),
            exceptions=(TimeoutError, ConnectionError, Exception),
            max_attempts=2,
            timeout_seconds=5
        )
        
        await retryable(
            recipient_economy.add_currency,
            args=(amount, "received", {"sender_id": player_id, "sender_name": ctx.author.name}),
            exceptions=(TimeoutError, ConnectionError, Exception),
            max_attempts=2,
            timeout_seconds=5
        )

        # Create embed
        embed = await EmbedBuilder.create_base_embed(
            title="Credits Transfer",
            description=f"Successfully transferred {amount} credits to {user.mention}",
            guild=guild
        )

        # Get new balances
        player_new_balance = await retryable(
            player_economy.get_balance,
            exceptions=(TimeoutError, ConnectionError, Exception),
            max_attempts=2,
            timeout_seconds=5
        )
        
        embed.add_field(name="Your New Balance", value=f"{player_new_balance} credits", inline=False)

        # Send with economy icon
        from utils.embed_icons import send_embed_with_icon, ECONOMY_ICON
        return await send_embed_with_icon(ctx, embed, ECONOMY_ICON)

    @economy.command(name="adjust", description="Add or remove credits from a player (Admin only)")
    @app_commands.describe(
        server_id="Select a server by name",
        user="The user to adjust credits for",
        amount="The amount to add (positive) or remove (negative)",
        reason="Reason for the adjustment"
    )
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    @has_admin_permission()
    @premium_tier_required(2)  # Economy requires premium tier 2+
    async def adjust_credits(self, ctx, server_id: str, user: discord.Member, amount: int, reason: str = "Admin adjustment"):
        """Add or remove credits from a player (Admin only)"""
        # Get guild and server data 
        guild = await Guild.get_guild(self.bot.db, ctx.guild.id)
        server = await guild.get_server(server_id)
        server_name = server.get("server_name", server_id)

        # Validate amount - can be any integer except 0
        if amount == 0:
            embed = await EmbedBuilder.create_error_embed(
                "Invalid Amount",
                "Amount must be non-zero. Use positive values to add and negative to remove credits.",
                guild=guild
            )
            return await ctx.send(embed=embed)

        # Get player data with retryable for network resilience
        player_id = str(user.id)
        economy = await retryable(
            EconomyModel.get_by_player,
            args=(self.bot.db, player_id, server_id),
            exceptions=(TimeoutError, ConnectionError, Exception),
            max_attempts=2,
            timeout_seconds=5
        )

        if not economy:
            # Create new economy account
            economy = await retryable(
                EconomyModel.create_or_update,
                args=(self.bot.db, player_id, server_id),
                exceptions=(TimeoutError, ConnectionError, Exception),
                max_attempts=2,
                timeout_seconds=5
            )

        # Get initial balance for reporting
        initial_balance = await retryable(
            economy.get_balance,
            exceptions=(TimeoutError, ConnectionError, Exception),
            max_attempts=2,
            timeout_seconds=5
        )

        # Add or remove currency with retryable
        if amount > 0:
            await retryable(
                economy.add_currency,
                args=(amount, "admin_adjustment", {
                    "admin_id": str(ctx.author.id),
                    "admin_name": ctx.author.name,
                    "reason": reason
                }),
                exceptions=(TimeoutError, ConnectionError, Exception),
                max_attempts=2,
                timeout_seconds=5
            )
            action_text = "Added"
        else:
            # Absolute value for remove_currency
            removal_result = await retryable(
                economy.remove_currency,
                args=(abs(amount), "admin_adjustment", {
                    "admin_id": str(ctx.author.id),
                    "admin_name": ctx.author.name,
                    "reason": reason
                }),
                exceptions=(TimeoutError, ConnectionError, Exception),
                max_attempts=2,
                timeout_seconds=5
            )

            if not removal_result:
                embed = await EmbedBuilder.create_error_embed(
                    "Insufficient Funds",
                    f"Player only has {initial_balance} credits. Cannot remove {abs(amount)} credits.",
                    guild=guild
                )
                return await ctx.send(embed=embed)

            action_text = "Removed"

        # Get new balance with retryable
        new_balance = await retryable(
            economy.get_balance,
            exceptions=(TimeoutError, ConnectionError, Exception),
            max_attempts=2,
            timeout_seconds=5
        )

        # Create success embed
        embed = await EmbedBuilder.create_base_embed(
            title="Credits Adjustment",
            description=f"{action_text} {abs(amount)} credits {'to' if amount > 0 else 'from'} {user.mention}",
            guild=guild
        )

        embed.add_field(name="Previous Balance", value=f"{initial_balance} credits", inline=True)
        embed.add_field(name="New Balance", value=f"{new_balance} credits", inline=True)
        embed.add_field(name="Reason", value=reason, inline=False)

        # Send with economy icon
        from utils.embed_icons import send_embed_with_icon, ECONOMY_ICON
        
        # Log the adjustment
        logger.info(f"Admin {ctx.author.name} ({ctx.author.id}) {action_text.lower()} {abs(amount)} credits {'to' if amount > 0 else 'from'} {user.name} ({user.id}) on server {server_name} ({server_id})")
        
        return await send_embed_with_icon(ctx, embed, ECONOMY_ICON)

    @economy.command(name="transactions", description="View your transaction history")
    @app_commands.describe(
        server_id="Select a server by name",
        user="The user to view transactions for (admins only)",
        limit="Maximum number of transactions to show"
    )
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    @premium_tier_required(2)  # Economy requires premium tier 2+
    async def transactions(self, ctx, server_id: str, user: Optional[discord.Member] = None, limit: int = 10):
        """View your transaction history or another user's (admin only)"""
        # Get guild and server data
        guild = await Guild.get_guild(self.bot.db, ctx.guild.id)
        server = await guild.get_server(server_id)
        server_name = server.get("server_name", server_id)

        # If user is specified and not self, verify admin permissions
        target_user = user or ctx.author
        if user and user.id != ctx.author.id:
            from utils.helpers import has_admin_permission
            if not has_admin_permission(ctx):
                embed = await EmbedBuilder.create_error_embed(
                    "Permission Denied",
                    "You can only view your own transactions unless you're an admin.",
                    guild=guild
                )
                return await ctx.send(embed=embed, ephemeral=True)

        # Limit the maximum number of transactions to retrieve
        if limit < 1:
            limit = 1
        elif limit > 25:
            limit = 25

        # Get player data with retryable for network resilience
        player_id = str(target_user.id)
        economy = await retryable(
            EconomyModel.get_by_player,
            args=(self.bot.db, player_id, server_id),
            exceptions=(TimeoutError, ConnectionError, Exception),
            max_attempts=2,
            timeout_seconds=5
        )

        if not economy:
            # Create new economy account
            economy = await retryable(
                EconomyModel.create_or_update,
                args=(self.bot.db, player_id, server_id),
                exceptions=(TimeoutError, ConnectionError, Exception),
                max_attempts=2,
                timeout_seconds=5
            )

        # Get transaction history with retryable
        transactions = await retryable(
            economy.get_recent_transactions,
            args=(limit,),
            exceptions=(TimeoutError, ConnectionError, Exception),
            max_attempts=2,
            timeout_seconds=5
        )
        
        current_balance = await retryable(
            economy.get_balance,
            exceptions=(TimeoutError, ConnectionError, Exception),
            max_attempts=2,
            timeout_seconds=5
        )

        # Create embed for transaction history
        embed = await EmbedBuilder.create_base_embed(
            title="Transaction History",
            description=f"Recent transactions for {target_user.mention} on {server_name}",
            guild=guild
        )

        embed.add_field(name="Current Balance", value=f"{current_balance} credits", inline=False)

        if not transactions or len(transactions) == 0:
            embed.add_field(name="No Transactions", value="No transaction history found", inline=False)
        else:
            # Format transactions
            for i, tx in enumerate(transactions[:10], 1):  # Show at most 10 in embed
                # Get transaction details with error handling
                amount = tx.get("amount", 0)
                tx_type = tx.get("type", "unknown")
                source = tx.get("source", "unknown")
                balance = tx.get("balance", 0)
                timestamp = tx.get("timestamp")

                # Format timestamp
                if timestamp:
                    if isinstance(timestamp, str):
                        try:
                            timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                        except ValueError:
                            timestamp = None

                    if timestamp:
                        time_str = f"<t:{int(timestamp.timestamp())}:R>"
                    else:
                        time_str = "Unknown time"
                else:
                    time_str = "Unknown time"

                # Format details based on source
                details = ""
                if source == "daily_reward":
                    details = "Daily reward claimed"
                elif source == "gambling":
                    game = tx.get("details", {}).get("game", "unknown")
                    result = tx.get("details", {}).get("result", "unknown")
                    details = f"{game.capitalize()} - {result.capitalize()}"
                elif source == "admin_adjustment":
                    admin_name = tx.get("details", {}).get("admin_name", "Unknown")
                    reason = tx.get("details", {}).get("reason", "No reason provided")
                    details = f"By {admin_name}: {reason}"
                elif source == "interest":
                    rate = tx.get("details", {}).get("rate", 0) * 100
                    details = f"Weekly interest at {rate}%"
                elif source == "transfer":
                    if tx_type == "credit":
                        from_name = tx.get("details", {}).get("from_name", "Unknown")
                        details = f"From {from_name}"
                    else:  # debit
                        to_name = tx.get("details", {}).get("to_name", "Unknown")
                        details = f"To {to_name}"
                elif source == "received":  # Added support for received transactions
                    sender_name = tx.get("details", {}).get("sender_name", "Unknown")
                    details = f"From {sender_name}"

                # Format field
                sign = "+" if tx_type == "credit" else "-"
                field_name = f"{i}. {sign}{abs(amount)} credits - {time_str}"
                field_value = f"**{source.replace('_', ' ').title()}**: {details}\nBalance: {balance} credits"
                
                embed.add_field(name=field_name, value=field_value, inline=False)

            # Add note if there are more transactions
            if len(transactions) > 10:
                embed.add_field(
                    name="Note",
                    value=f"Showing 10 of {len(transactions)} transactions. Use a higher limit to see more.",
                    inline=False
                )

        # Send with economy icon
        from utils.embed_icons import send_embed_with_icon, ECONOMY_ICON
        return await send_embed_with_icon(ctx, embed, ECONOMY_ICON)

    @economy.command(name="stats", description="View economy statistics for a server")
    @app_commands.describe(
        server_id="Select a server by name"
    )
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    @has_admin_permission()
    @premium_tier_required(2)  # Economy requires premium tier 2+
    async def economy_stats(self, ctx, server_id: str):
        """View economy statistics for a server (Admin only)"""
        # Get guild and server data
        guild = await Guild.get_guild(self.bot.db, ctx.guild.id)
        server = await guild.get_server(server_id)
        server_name = server.get("server_name", server_id)

        # Get economy statistics with retryable to handle network issues
        stats = await retryable(
            EconomyModel.get_economy_stats,
            args=(self.bot.db, server_id),
            exceptions=(TimeoutError, ConnectionError, Exception),
            max_attempts=2,
            timeout_seconds=10  # Longer timeout for this potentially resource-intensive operation
        )
        
        # Ensure stats is not None
        if not stats:
            stats = {}  # Use an empty dict as fallback for safe access
            logger.warning(f"No economy statistics found for server {server_id}")

        # Create embed for economy stats
        embed = await EmbedBuilder.create_base_embed(
            title="Economy Statistics",
            description=f"Economy statistics for {server_name}",
            guild=guild
        )

        # General stats with safe access
        embed.add_field(
            name="General Statistics", 
            value=f"Total Currency: {stats.get('total_currency', 0):,} credits\n"
                  f"Lifetime Earnings: {stats.get('total_lifetime_earnings', 0):,} credits\n"
                  f"Active Accounts: {stats.get('active_accounts', 0):,}",
            inline=False
        )

        # Gambling stats with robust error handling
        try:
            blackjack = stats.get('gambling_stats', {}).get('blackjack', {'wins': 0, 'losses': 0, 'earnings': 0})
            slots = stats.get('gambling_stats', {}).get('slots', {'wins': 0, 'losses': 0, 'earnings': 0})
            
            # Calculate win rates safely (avoid division by zero)
            bj_games = blackjack.get('wins', 0) + blackjack.get('losses', 0)
            bj_win_rate = (blackjack.get('wins', 0) / bj_games) * 100 if bj_games > 0 else 0.0

            slots_games = slots.get('wins', 0) + slots.get('losses', 0)
            slots_win_rate = (slots.get('wins', 0) / slots_games) * 100 if slots_games > 0 else 0.0

            gambling_text = (
                f"**Blackjack**\n"
                f"Games Played: {bj_games:,}\n"
                f"Wins: {blackjack.get('wins', 0):,} | Losses: {blackjack.get('losses', 0):,}\n"
                f"Win Rate: {bj_win_rate:.1f}%\n"
                f"Player Earnings: {blackjack.get('earnings', 0):,} credits\n\n"

                f"**Slots**\n"
                f"Games Played: {slots_games:,}\n"
                f"Wins: {slots.get('wins', 0):,} | Losses: {slots.get('losses', 0):,}\n"
                f"Win Rate: {slots_win_rate:.1f}%\n"
                f"Player Earnings: {slots.get('earnings', 0):,} credits"
            )
            embed.add_field(name="Gambling Statistics", value=gambling_text, inline=False)
        except Exception as e:
            logger.error(f"Error processing gambling stats: {e}", exc_info=True)
            embed.add_field(name="Gambling Statistics", value="Error retrieving gambling statistics", inline=False)

        # Transaction sources with error handling
        try:
            sources = stats.get('transaction_sources', {})
            if sources and len(sources) > 0:
                sources_text = ""
                # Safely handle sorting with default values
                try:
                    sorted_sources = sorted(sources.items(), key=lambda x: x[1].get('count', 0) if isinstance(x[1], dict) else 0, reverse=True)[:5]
                    for source, data in sorted_sources:
                        if not isinstance(data, dict):
                            continue
                        source_name = source.replace('_', ' ').title()
                        sources_text += f"**{source_name}**\n"
                        sources_text += f"Count: {data.get('count', 0):,} transactions\n"
                        sources_text += f"Credits In: {data.get('credit', 0):,} | Credits Out: {data.get('debit', 0):,}\n\n"

                    embed.add_field(name="Top Transaction Sources", value=sources_text or "No detailed transaction data available", inline=False)
                except Exception as sort_err:
                    logger.error(f"Error sorting transaction sources: {sort_err}", exc_info=True)
                    # Fallback to simpler display
                    simple_text = "\n".join([f"{k.replace('_', ' ').title()}: {v.get('count', 0) if isinstance(v, dict) else v:,}" 
                                          for k, v in sources.items()])
                    embed.add_field(name="Transaction Sources", value=simple_text or "Error processing transaction data", inline=False)
            else:
                embed.add_field(name="Transaction Sources", value="No transactions recorded yet", inline=False)
        except Exception as e:
            logger.error(f"Error processing transaction sources: {e}", exc_info=True)
            embed.add_field(name="Transaction Sources", value="Error retrieving transaction sources", inline=False)

        # Send with economy icon
        from utils.embed_icons import send_embed_with_icon, ECONOMY_ICON
        return await send_embed_with_icon(ctx, embed, ECONOMY_ICON)

    @gambling.command(name="roulette", description="Play roulette")
    @app_commands.describe(
        server_id="Select a server by name to play on",
        bet="The amount to bet (default: 10)"
    )
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    @premium_tier_required(2)  # Gambling requires premium tier 2+
    async def roulette(self, ctx, server_id: str, bet: int = 10):
        """Play roulette"""
        try:
            # Get guild model for themed embed
            guild_data = None
            guild_model = None
            try:
                guild_data = await self.bot.db.guilds.find_one({"guild_id": ctx.guild.id})
                if guild_data:
                    # Use create_from_db_document to ensure proper conversion of premium_tier
                    guild_model = Guild.create_from_db_document(guild_data, self.bot.db)
            except Exception as e:
                logger.warning(f"Error getting guild model: {e}")

            # Get guild model for the command
            guild = await Guild.get_guild(self.bot.db, ctx.guild.id)

            # Validate server ID
            server = await guild.get_server(server_id)
            if not server:
                embed = await EmbedBuilder.create_error_embed(
                    "Invalid Server",
                    f"Could not find server with ID '{server_id}'. "
                    f"Please use `/server list` to see available servers.",
                    guild=guild
                )
                await ctx.send(embed=embed)
                return

            # Validate bet amount
            if bet <= 0:
                embed = await EmbedBuilder.create_error_embed(
                    "Invalid Bet",
                    "Bet amount must be greater than 0.",
                    guild=guild
                )
                await ctx.send(embed=embed)
                return

            # Create economy instance for this player in this server
            economy = await PlayerEconomy.get_or_create(
                self.bot.db,
                ctx.guild.id,
                ctx.author.id,
                server["_id"]
            )

            # Check if player has enough currency
            balance = await economy.get_balance()
            if balance < bet:
                embed = await EmbedBuilder.create_error_embed(
                    "Insufficient Funds",
                    f"You don't have enough credits to place this bet. "
                    f"Your current balance is {balance} credits.",
                    guild=guild
                )
                await ctx.send(embed=embed)
                return

            # Create roulette game
            from utils.gambling import RouletteView
            view = RouletteView(str(ctx.author.id), economy, bet)

            # Send initial embed
            embed = discord.Embed(
                title="🎲 Roulette 🎲",
                description=f"Place your bet: {bet} credits",
                color=discord.Color.blue()
            )

            embed.add_field(
                name="Your Balance",
                value=f"{balance} credits",
                inline=False
            )

            embed.add_field(
                name="How to Play",
                value="Select a bet type from the dropdown menu.",
                inline=False
            )

            # Send message with view
            message = await ctx.send(embed=embed, view=view)
            view.message = message

        except Exception as e:
            logger.error(f"Error playing roulette: {e}", exc_info=True)
            embed = await EmbedBuilder.create_error_embed(
                "Error",
                f"An error occurred while playing roulette: {e}",
                guild=guild
            )
            await ctx.send(embed=embed)

    @classmethod
    async def get_richest_players(cls, db, server_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get the richest players on a server"""
        return await EconomyModel.get_richest_players(db, server_id, limit)

    @classmethod
    async def get_economy_stats(cls, db, server_id: str) -> Dict[str, Any]:
        """Get economy statistics for a server"""
        return await EconomyModel.get_economy_stats(db, server_id)


async def setup(bot):
    """Set up the Economy cog"""
    # Import here to avoid circular import
    from datetime import datetime
    from models.economy import Economy as EconomyModel
    await bot.add_cog(Economy(bot))