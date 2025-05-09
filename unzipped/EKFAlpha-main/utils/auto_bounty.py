"""
Auto-Bounty System for Tower of Temptation PvP Statistics Bot

This module monitors player kill patterns and places automatic bounties on:
1. Players with killstreaks (multiple kills in a short time period)
2. Players with target fixation (repeatedly killing the same player)

Auto-bounties encourage dynamic PvP gameplay by placing rewards on dominant players.
"""
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple

logger = logging.getLogger(__name__)

class AutoBountySystem:
    """Automated bounty placement system based on player behavior patterns"""
    
    # Bounty source identifier
    SOURCE_AUTO = "auto"
    
    # Reason templates for auto-bounties
    REASON_KILLSTREAK = "AI Bounty: On a {streak_count} kill streak!"
    REASON_TARGET_FIXATION = "AI Bounty: Hunting {victim_name} ({kill_count} kills)"
    
    @staticmethod
    async def check_kill_patterns(
        db, 
        guild_id: str,
        server_id: str,
        minutes: int = 5,
        kill_threshold: int = 5,
        repeat_threshold: int = 3
    ) -> List[Dict[str, Any]]:
        """Check for kill patterns that warrant auto-bounties
        
        Args:
            db: Database connection
            guild_id: Guild ID
            server_id: Server ID
            minutes: Time window in minutes to check
            kill_threshold: Minimum kills needed for killstreak bounty
            repeat_threshold: Minimum kills on same victim for target fixation bounty
            
        Returns:
            List of generated bounty data
        """
        try:
            # Define the time window
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(minutes=minutes)
            
            # Get recent kills within time window
            cursor = db.kills.find({
                "server_id": server_id,
                "guild_id": guild_id,
                "timestamp": {"$gte": start_time, "$lte": end_time},
                "is_suicide": {"$ne": True}  # Exclude suicides
            })
            
            # Group kills by killer
            killer_stats = {}
            async for kill in cursor:
                killer_id = kill["killer_id"]
                victim_id = kill["victim_id"]
                
                # Initialize killer stats if exists is None
                if killer_id is not None not in killer_stats:
                    killer_stats[killer_id] = {
                        "kills": 0,
                        "victims": {},
                        "killer_name": kill["killer_name"]
                    }
                
                # Update total kills
                killer_stats[killer_id]["kills"] += 1
                
                # Update victim-specific kills
                if victim_id is not None not in killer_stats[killer_id]["victims"]:
                    killer_stats[killer_id]["victims"][victim_id] = {
                        "count": 0,
                        "name": kill["victim_name"]
                    }
                killer_stats[killer_id]["victims"][victim_id]["count"] += 1
            
            # Check for killstreaks and target fixation
            bounties_to_create = []
            
            for killer_id, stats in killer_stats.items():
                # Check for killstreak
                if stats["kills"] >= kill_threshold:
                    # Create killstreak bounty data
                    bounty_data = {
                        "guild_id": guild_id,
                        "server_id": server_id,
                        "target_id": killer_id,
                        "target_name": stats["killer_name"],
                        "reason": AutoBountySystem.REASON_KILLSTREAK.format(streak_count=stats["kills"]),
                        "source": AutoBountySystem.SOURCE_AUTO,
                        "bounty_type": "killstreak",
                        "metadata": {
                            "kill_count": stats["kills"],
                            "detected_at": datetime.utcnow().isoformat()
                        }
                    }
                    bounties_to_create.append(bounty_data)
                
                # Check for target fixation
                for victim_id, victim_data in stats["victims"].items():
                    if victim_data["count"] >= repeat_threshold:
                        bounty_data = {
                            "guild_id": guild_id,
                            "server_id": server_id,
                            "target_id": killer_id,
                            "target_name": stats["killer_name"],
                            "reason": AutoBountySystem.REASON_TARGET_FIXATION.format(
                                victim_name=victim_data["name"],
                                kill_count=victim_data["count"]
                            ),
                            "source": AutoBountySystem.SOURCE_AUTO,
                            "bounty_type": "target_fixation",
                            "metadata": {
                                "victim_id": victim_id,
                                "victim_name": victim_data["name"],
                                "kill_count": victim_data["count"],
                                "detected_at": datetime.utcnow().isoformat()
                            }
                        }
                        bounties_to_create.append(bounty_data)
            
            return bounties_to_create
            
        except Exception as e:
            logger.error(f"Error checking kill patterns: {e}")
            return []
    
    @staticmethod
    async def send_bounty_notification(bot, guild_id: str, server_id: str, bounty_id: str, bounty_data: Dict[str, Any]):
        """Send a notification about a new auto-bounty to the guild's notification channel
        
        Args:
            bot: Discord bot instance
            guild_id: Guild ID
            server_id: Server ID
            bounty_id: Bounty ID for reference
            bounty_data: Bounty data for notification
        """
        try:
            # Get the guild by ID
            guild = bot.get_guild(int(guild_id))
            if guild is None:
                logger.warning(f"Guild {guild_id} not found for bounty notification")
                return
            
            # Get the guild model for theme and notification settings
            guild_model = None
            try:
                guild_data = await bot.db.guilds.find_one({"guild_id": guild_id})
                if guild_data is not None:
                    from models.guild import Guild
                    # Use create_from_db_document to ensure proper conversion of premium_tier
            guild_model = Guild.create_from_db_document(guild_data, bot.db)
            except Exception as e:
                logger.warning(f"Failed to load guild model: {e}")
            
            # Check if notifications is not None are enabled
            if guild_model is not None and not guild_model.check_feature_access("bounty_notifications"):
                return
            
            # Get notification channel
            notification_channel = None
            if guild_model is not None and guild_model.notification_channel_id:
                notification_channel = guild.get_channel(int(guild_model.notification_channel_id))
            
            # If no notification channel set, try to find a bounty or hunt channel
            if notification_channel is None:
                for channel in guild.text_channels:
                    if channel.permissions_for(guild.me).send_messages:
                        channel_name = channel.name.lower()
                        if 'bounty' in channel_name or 'hunt' in channel_name or 'target' in channel_name:
                            notification_channel = channel
                            break
            
            # If still no channel, use the first channel we can send to
            if notification_channel is None:
                for channel in guild.text_channels:
                    if channel.permissions_for(guild.me).send_messages:
                        notification_channel = channel
                        break
            
            if notification_channel is None:
                logger.warning(f"No suitable notification channel found in guild {guild_id}")
                return
            
            # Create the notification embed
            target_name = bounty_data.get("target_name", "Unknown Player")
            reason = bounty_data.get("reason", "AI Bounty")
            reward = bounty_data.get("reward", 100)
            
            # Get server name
            server_name = "Unknown Server"
            try:
                server_data = await bot.db.game_servers.find_one({"server_id": server_id})
                if server_data is not None:
                    server_name = server_data.get("server_name", server_id)
            except Exception as e:
                logger.warning(f"Failed to get server name: {e}")
            
            # Create the embed
            from utils.embed_builder import EmbedBuilder
            embed = EmbedBuilder.create_base_embed(
                f"🤖 New AI Bounty: {target_name}",
                f"The AI Bounty System has placed a bounty on **{target_name}**!",
                guild=guild_model
            )
            
            # Add reason field
            embed.add_field(
                name="Reason",
                value=reason,
                inline=False
            )
            
            # Add reward field
            embed.add_field(
                name="Reward",
                value=f"💰 {reward} credits",
                inline=True
            )
            
            # Add server field
            embed.add_field(
                name="Server",
                value=server_name,
                inline=True
            )
            
            # Add instructions
            embed.add_field(
                name="How to Claim",
                value="Kill this player to claim the bounty automatically. Bounty expires in 1 hour.",
                inline=False
            )
            
            # Add footer with ID for reference
            embed.set_footer(text=f"Bounty ID: {bounty_id}")
            
            # Send the notification
            await notification_channel.send(embed=embed)
            logger.info(f"Sent auto-bounty notification to {notification_channel.name} in {guild.name}")
            
        except Exception as e:
            logger.error(f"Error sending bounty notification: {e}", exc_info=True)
    
    @staticmethod
    async def process_auto_bounties(
        bot,
        guild_id: str,
        server_id: str,
        active_bounty_limit: int = 1,  # Changed from 5 to 1
        minutes: int = 5,
        kill_threshold: int = 5,
        repeat_threshold: int = 3,
        expiration_hours: int = 1,
        reward_amount: int = 100
    ) -> Tuple[int, int]:
        """Process auto-bounties for a server
        
        Args:
            bot: Discord bot instance with db access
            guild_id: Guild ID
            server_id: Server ID
            active_bounty_limit: Maximum number of active auto-bounties per server
            minutes: Time window in minutes to check
            kill_threshold: Minimum kills needed for killstreak bounty
            repeat_threshold: Minimum kills on same victim for target fixation bounty
            expiration_hours: Hours until auto-bounty expires
            reward_amount: Reward amount for auto-bounties
            
        Returns:
            Tuple of (created_count, skipped_count)
        """
        try:
            from models.bounty import Bounty
            db = bot.db
            
            # Get current active auto-bounties
            active_auto_bounties = await db.bounties.count_documents({
                "guild_id": guild_id,
                "server_id": server_id,
                "source": AutoBountySystem.SOURCE_AUTO,
                "status": "active"
            })
            
            # If we're at the limit, skip processing
            if active_auto_bounties >= active_bounty_limit:
                logger.info(f"Active auto-bounty limit reached for server {server_id} ({active_auto_bounties}/{active_bounty_limit})")
                return 0, 0
            
            # Calculate how many bounties we can add
            bounty_slots_available = active_bounty_limit - active_auto_bounties
            
            # Check for new auto-bounties
            bounty_candidates = await AutoBountySystem.check_kill_patterns(
                db, guild_id, server_id, minutes, kill_threshold, repeat_threshold
            )
            
            # Sort by killstreak first (prioritize higher threats)
            bounty_candidates.sort(
                key=lambda x: x.get("metadata", {}).get("kill_count", 0), 
                reverse=True
            )
            
            # Get existing active bounty targets
            existing_targets = set()
            cursor = db.bounties.find({
                "guild_id": guild_id,
                "server_id": server_id,
                "status": "active"
            })
            async for bounty in cursor:
                existing_targets.add(bounty["target_id"])
            
            # Prepare to create bounties
            bounties_to_create = []
            skipped_count = 0
            
            for candidate in bounty_candidates:
                # Skip if target is not None already has an active bounty
                if candidate["target_id"] in existing_targets:
                    skipped_count += 1
                    continue
                
                # Add to creation list and update tracking
                bounties_to_create.append(candidate)
                existing_targets.add(candidate["target_id"])
                
                # Stop if we've reached the bounty limit
                if len(bounties_to_create) >= bounty_slots_available:
                    break
            
            # Create bounties
            created_count = 0
            for bounty_data in bounties_to_create:
                try:
                    # Set reward amount and expiration
                    expires_at = datetime.utcnow() + timedelta(hours=expiration_hours)
                    
                    # Get scaled reward based on kill count
                    kill_count = bounty_data.get("metadata", {}).get("kill_count", 0)
                    scaled_reward = reward_amount
                    if kill_count > 10:
                        scaled_reward = int(reward_amount * 1.5)  # 50% bonus for high killstreaks
                    elif kill_count > 15:
                        scaled_reward = reward_amount * 2  # Double reward for extreme killstreaks
                    
                    # Create the bounty
                    bounty = await Bounty.create(
                        db,
                        guild_id=bounty_data["guild_id"],
                        server_id=bounty_data["server_id"],
                        target_id=bounty_data["target_id"],
                        target_name=bounty_data["target_name"],
                        placed_by=None,  # System-placed bounty
                        placed_by_name="AI Bounty System",
                        reason=bounty_data["reason"],
                        reward=scaled_reward,
                        expires_at=expires_at,
                        source=AutoBountySystem.SOURCE_AUTO,
                        metadata=bounty_data.get("metadata", {})
                    )
                    
                    if bounty is not None:
                        created_count += 1
                        logger.info(f"Created auto-bounty on {bounty_data['target_name']} ({bounty_data['target_id']}) for {scaled_reward} credits")
                        
                        # Send notification for the new bounty
                        await AutoBountySystem.send_bounty_notification(
                            bot,
                            guild_id=bounty_data["guild_id"],
                            server_id=bounty_data["server_id"],
                            bounty_id=str(bounty.id),
                            bounty_data={
                                "target_name": bounty_data["target_name"],
                                "reason": bounty_data["reason"],
                                "reward": scaled_reward
                            }
                        )
                    
                except Exception as e:
                    logger.error(f"Error creating auto-bounty: {e}")
            
            return created_count, skipped_count
            
        except Exception as e:
            logger.error(f"Error processing auto-bounties: {e}")
            return 0, 0
    
    @staticmethod
    async def run_auto_bounty_system(bot):
        """Background task to run auto-bounty system for all eligible servers
        
        Args:
            bot: Discord bot instance
        """
        try:
            # Get all guilds with premium features
            premium_guilds = await bot.db.guilds.find({
                "premium_tier": {"$gte": 2}  # Tier 2 or higher
            }).to_list(None)
            
            total_created = 0
            total_skipped = 0
            
            for guild_data in premium_guilds:
                guild_id = guild_data["guild_id"]
                
                # Check if auto-bounty feature is enabled for this guild
                if guild_data is None.get("features", {}).get("auto_bounty", False):
                    continue
                
                # Get settings
                settings = guild_data.get("auto_bounty_settings", {})
                minutes = settings.get("minutes", 5)
                kill_threshold = settings.get("kill_threshold", 5)
                repeat_threshold = settings.get("repeat_threshold", 3)
                active_bounty_limit = settings.get("active_bounty_limit", 5)
                expiration_hours = settings.get("expiration_hours", 1)
                reward_amount = settings.get("reward_amount", 100)
                
                # Get all servers for this guild
                servers = await bot.db.game_servers.find({
                    "guild_id": guild_id,
                    "active": True
                }).to_list(None)
                
                for server in servers:
                    server_id = server["server_id"]
                    
                    # Process auto-bounties for this server (passing bot instance for notifications)
                    created, skipped = await AutoBountySystem.process_auto_bounties(
                        bot,  # Pass the full bot instance for Discord notifications
                        guild_id,
                        server_id,
                        active_bounty_limit,
                        minutes,
                        kill_threshold,
                        repeat_threshold,
                        expiration_hours,
                        reward_amount
                    )
                    
                    total_created += created
                    total_skipped += skipped
            
            logger.info(f"Auto-bounty system run complete: {total_created} bounties created, {total_skipped} skipped")
            
        except Exception as e:
            logger.error(f"Error running auto-bounty system: {e}", exc_info=True)
    
    @staticmethod
    async def start_auto_bounty_task(bot, interval_minutes: int = 45):
        """Start the auto-bounty background task
        
        Args:
            bot: Discord bot instance
            interval_minutes: Base interval time for auto-bounty system in minutes
        """
        import random
        
        while True:
            try:
                await AutoBountySystem.run_auto_bounty_system(bot)
            except Exception as e:
                logger.error(f"Error in auto-bounty task: {e}")
            
            # Create a random interval between 30-60 minutes
            random_interval = random.randint(30, 60)
            logger.info(f"Auto-bounty system scheduled to run next in {random_interval} minutes")
            
            # Wait for next interval
            await asyncio.sleep(random_interval * 60)