"""
Embed Builder for the Tower of Temptation PvP Statistics Discord Bot.

This module provides:
1. Standard embed generation
2. Themed embed styles
3. Common embed layouts
4. Utility functions for embed creation
"""
import logging
import random
from datetime import datetime
from typing import Dict, List, Optional, Any, Union, Tuple

import discord

logger = logging.getLogger(__name__)

class EmbedBuilder:
    """Utility class for building Discord embeds"""
    
    # Embed color palette
    COLORS = {
        "primary": 0x3498db,    # Blue
        "success": 0x2ecc71,    # Green
        "warning": 0xf39c12,    # Orange
        "error": 0xe74c3c,      # Red
        "info": 0x9b59b6,       # Purple
        "neutral": 0x95a5a6,    # Gray
        "faction_a": 0xe74c3c,  # Red for Faction A
        "faction_b": 0x3498db,  # Blue for Faction B
        
        # Additional colors for variety
        "gold": 0xf1c40f,
        "silver": 0xbdc3c7,
        "bronze": 0xcd6133,
        "emerald": 0x2ecc71,
        "ruby": 0xe74c3c,
        "sapphire": 0x3498db,
        "amethyst": 0x9b59b6,
        "topaz": 0xf39c12,
        "diamond": 0x1abc9c,
    }
    
    @staticmethod
    def error(title: Optional[str] = None, description: Optional[str] = None, **kwargs) -> discord.Embed:
        """Create an error-themed embed
        
        Args:
            title: Embed title (default: Error)
            description: Embed description
            **kwargs: Additional arguments for embed
            
        Returns:
            discord.Embed: Error-themed embed
        """
        embed = discord.Embed(
            title=title or "Error",
            description=description,
            color=EmbedBuilder.COLORS["error"]
        )
        return embed
        
    @staticmethod
    def success(title: Optional[str] = None, description: Optional[str] = None, **kwargs) -> discord.Embed:
        """Create a success-themed embed
        
        Args:
            title: Embed title (default: Success)
            description: Embed description
            **kwargs: Additional arguments for embed
            
        Returns:
            discord.Embed: Success-themed embed
        """
        embed = discord.Embed(
            title=title or "Success",
            description=description,
            color=EmbedBuilder.COLORS["success"]
        )
        return embed
        
    @staticmethod
    def info(title: Optional[str] = None, description: Optional[str] = None, **kwargs) -> discord.Embed:
        """Create an info-themed embed
        
        Args:
            title: Embed title (default: Information)
            description: Embed description
            **kwargs: Additional arguments for embed
            
        Returns:
            discord.Embed: Info-themed embed
        """
        embed = discord.Embed(
            title=title or "Information",
            description=description,
            color=EmbedBuilder.COLORS["info"]
        )
        return embed
    
    # Common icons
    ICONS = {
        "success": "https://i.imgur.com/FcaXvqo.png",  # Green checkmark
        "warning": "https://i.imgur.com/rYMeoCZ.png",  # Yellow warning
        "error": "https://i.imgur.com/gfo8TJj.png",    # Red error
        "info": "https://i.imgur.com/wMFN7Qj.png",     # Blue info
        "neutral": "https://i.imgur.com/ViHN3X2.png",  # Gray neutral
        "sword": "https://i.imgur.com/JGocbFP.png",    # Sword icon
        "shield": "https://i.imgur.com/4HkY3BB.png",   # Shield icon
        "trophy": "https://i.imgur.com/lPJeQXG.png",   # Trophy icon
        "skull": "https://i.imgur.com/X8QUQxS.png",    # Skull icon
        "crown": "https://i.imgur.com/TzUnLSU.png",    # Crown icon
        "stats": "https://i.imgur.com/YVgjUHM.png",    # Stats icon
        "settings": "https://i.imgur.com/K4JZrZ4.png", # Settings icon
        "faction_a": "https://i.imgur.com/DLXqXXa.png", # Faction A icon (placeholder)
        "faction_b": "https://i.imgur.com/2uDQ7m1.png", # Faction B icon (placeholder)
    }
    
    @classmethod
    async def ensure_field_limits(cls, fields: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        """Ensure embed field limits are respected"""
        if fields is None:
            return []
            
        processed_fields = []
        for field in fields:
            name = str(field.get("name", ""))[:256]  # Discord limit
            value = str(field.get("value", ""))[:1024]  # Discord limit
            processed_fields.append({
                "name": name,
                "value": value,
                "inline": field.get("inline", False)
            })
            if len(processed_fields) >= 25:  # Discord's field limit
                break
        return processed_fields

    @classmethod
    async def validate_embed_limits(cls, title: Optional[str] = None, description: Optional[str] = None) -> Tuple[Optional[str], Optional[str]]:
        """Validate and truncate embed title/description to Discord limits
        
        Args:
            title: Embed title
            description: Embed description
            
        Returns:
            Tuple of (validated_title, validated_description)
        """
        MAX_TITLE_LENGTH = 256
        MAX_DESC_LENGTH = 4096
        
        # Validate title
        if title is not None:
            title = str(title)[:MAX_TITLE_LENGTH]
            
        # Validate description 
        if description is not None:
            description = str(description)[:MAX_DESC_LENGTH]
            
        return title, description
    
    @classmethod
    async def create_embed(cls, 
                          title: Optional[str] = None, 
                          description: Optional[str] = None,
                          color: Optional[int] = None,
                          fields: Optional[List[Dict[str, Any]]] = None,
                          thumbnail_url: Optional[str] = None,
                          image_url: Optional[str] = None,
                          author_name: Optional[str] = None,
                          author_url: Optional[str] = None,
                          author_icon_url: Optional[str] = None,
                          footer_text: Optional[str] = None,
                          footer_icon_url: Optional[str] = None,
                          timestamp: Optional[datetime] = None,
                          url: Optional[str] = None,
                          guild: Optional[discord.Guild] = None,
                          bot: Optional[discord.Client] = None) -> discord.Embed:
        """Create a Discord embed with the given parameters
        
        Args:
            title: Embed title (default: None)
            description: Embed description (default: None)
            color: Embed color (default: None)
            fields: List of field dictionaries with name, value, and inline keys (default: None)
            thumbnail_url: URL for thumbnail image (default: None)
            image_url: URL for main image (default: None)
            author_name: Name for author field (default: None)
            author_url: URL for author field (default: None)
            author_icon_url: Icon URL for author field (default: None)
            footer_text: Text for footer (default: None)
            footer_icon_url: Icon URL for footer (default: None)
            timestamp: Timestamp to display (default: None)
            url: URL for title (default: None)
            
        Returns:
            discord.Embed: Created embed
        """
        # Create embed with color or default
        embed = discord.Embed(color=color or cls.COLORS["primary"])
        
        # Set title and description if provided is not None
        if title is not None:
            embed.title = title
            
        if description is not None:
            embed.description = description
            
        if url is not None:
            embed.url = url
        
        # Add fields if provided is not None
        if fields is not None:
            for field in fields:
                embed.add_field(
                    name=field["name"],
                    value=field["value"],
                    inline=field.get("inline", False)
                )
        
        # Set thumbnail if provided is not None
        if thumbnail_url is not None:
            embed.set_thumbnail(url=thumbnail_url)
            
        # Set image if provided is not None
        if image_url is not None:
            embed.set_image(url=image_url)
            
        # Set author if provided is not None
        if author_name is not None:
            embed.set_author(
                name=author_name,
                url=author_url,
                icon_url=author_icon_url
            )
            
        # Set footer if provided
        if footer_text is not None:
            # If our footer contains "Powered By", let's use the bot's nickname if available
            from utils.helpers import get_bot_name
            
            if footer_text is not None and "Powered By" in footer_text and bot and guild:
                # Replace the standard bot name with the nickname if available
                bot_name = get_bot_name(bot, guild)
                footer_text = footer_text.replace("Discord Bot", bot_name)
            
            embed.set_footer(
                text=footer_text,
                icon_url=footer_icon_url
            )
            
        # Set timestamp if provided is not None
        if timestamp is not None:
            embed.timestamp = timestamp
        
        return embed
    
    @classmethod
    async def success_embed(cls, 
                           title: Optional[str] = None, 
                           description: Optional[str] = None,
                           thumbnail: bool = False,
                           guild: Optional[discord.Guild] = None,
                           bot: Optional[discord.Client] = None,
                           **kwargs) -> discord.Embed:
        """Create a success-themed embed
        
        Args:
            title: Embed title (default: None)
            description: Embed description (default: None)
            thumbnail: Whether to show success icon as thumbnail (default: False)
            guild: The Discord guild for customization (default: None)
            bot: The Discord bot instance for customization (default: None)
            **kwargs: Additional arguments for create_embed
            
        Returns:
            discord.Embed: Success-themed embed
        """
        # Set success color
        kwargs["color"] = cls.COLORS["success"]
        
        # Add success icon as thumbnail if requested
        if thumbnail is not None and "thumbnail_url" not in kwargs:
            kwargs["thumbnail_url"] = cls.ICONS["success"]
            
        # Set default title if provided is None
        if title is None:
            title = "Success"
        
        # Add guild and bot to kwargs if already is None present
        if "guild" in kwargs and guild is None:
            kwargs["guild"] = guild
            
        if "bot" in kwargs and bot is None:
            kwargs["bot"] = bot
            
        return await cls.create_embed(
            title=title,
            description=description,
            **kwargs
        )
    
    @classmethod
    async def error_embed(cls, 
                         title: Optional[str] = None, 
                         description: Optional[str] = None,
                         thumbnail: bool = False,
                         guild: Optional[discord.Guild] = None,
                         bot: Optional[discord.Client] = None,
                         **kwargs) -> discord.Embed:
        """Create an error-themed embed
        
        Args:
            title: Embed title (default: None)
            description: Embed description (default: None)
            thumbnail: Whether to show error icon as thumbnail (default: False)
            guild: The Discord guild for customization (default: None)
            bot: The Discord bot instance for customization (default: None)
            **kwargs: Additional arguments for create_embed
            
        Returns:
            discord.Embed: Error-themed embed
        """
        # Set error color
        kwargs["color"] = cls.COLORS["error"]
        
        # Add error icon as thumbnail if requested
        if thumbnail is not None and "thumbnail_url" not in kwargs:
            kwargs["thumbnail_url"] = cls.ICONS["error"]
            
        # Set default title if provided is None
        if title is None:
            title = "Error"
        
        # Add guild and bot to kwargs if already is None present
        if "guild" in kwargs and guild is None:
            kwargs["guild"] = guild
            
        if "bot" in kwargs and bot is None:
            kwargs["bot"] = bot
            
        return await cls.create_embed(
            title=title,
            description=description,
            **kwargs
        )
    
    @classmethod
    async def warning_embed(cls, 
                           title: Optional[str] = None, 
                           description: Optional[str] = None,
                           thumbnail: bool = False,
                           guild: Optional[discord.Guild] = None,
                           bot: Optional[discord.Client] = None,
                           **kwargs) -> discord.Embed:
        """Create a warning-themed embed
        
        Args:
            title: Embed title (default: None)
            description: Embed description (default: None)
            thumbnail: Whether to show warning icon as thumbnail (default: False)
            guild: The Discord guild for customization (default: None)
            bot: The Discord bot instance for customization (default: None)
            **kwargs: Additional arguments for create_embed
            
        Returns:
            discord.Embed: Warning-themed embed
        """
        # Set warning color
        kwargs["color"] = cls.COLORS["warning"]
        
        # Add warning icon as thumbnail if requested
        if thumbnail is not None and "thumbnail_url" not in kwargs:
            kwargs["thumbnail_url"] = cls.ICONS["warning"]
            
        # Set default title if provided is None
        if title is None:
            title = "Warning"
            
        # Add guild and bot to kwargs if already is None present
        if "guild" in kwargs and guild is None:
            kwargs["guild"] = guild
            
        if "bot" in kwargs and bot is None:
            kwargs["bot"] = bot
            
        return await cls.create_embed(
            title=title,
            description=description,
            **kwargs
        )
    
    @classmethod
    async def info_embed(cls, 
                        title: Optional[str] = None, 
                        description: Optional[str] = None,
                        thumbnail: bool = False,
                        guild: Optional[discord.Guild] = None,
                        bot: Optional[discord.Client] = None,
                        **kwargs) -> discord.Embed:
        """Create an info-themed embed
        
        Args:
            title: Embed title (default: None)
            description: Embed description (default: None)
            thumbnail: Whether to show info icon as thumbnail (default: False)
            guild: The Discord guild for customization (default: None)
            bot: The Discord bot instance for customization (default: None)
            **kwargs: Additional arguments for create_embed
            
        Returns:
            discord.Embed: Info-themed embed
        """
        # Set info color
        kwargs["color"] = cls.COLORS["info"]
        
        # Add info icon as thumbnail if requested
        if thumbnail is not None and "thumbnail_url" not in kwargs:
            kwargs["thumbnail_url"] = cls.ICONS["info"]
            
        # Set default title if provided is None
        if title is None:
            title = "Information"
            
        # Add guild and bot to kwargs if already is None present
        if "guild" in kwargs and guild is None:
            kwargs["guild"] = guild
            
        if "bot" in kwargs and bot is None:
            kwargs["bot"] = bot
            
        return await cls.create_embed(
            title=title,
            description=description,
            **kwargs
        )
    
    # Add alias methods for compatibility
    @classmethod
    async def create_error_embed(cls, 
                               title: Optional[str] = None, 
                               description: Optional[str] = None,
                               thumbnail: bool = False,
                               guild: Optional[discord.Guild] = None,
                               bot: Optional[discord.Client] = None,
                               **kwargs) -> discord.Embed:
        """Create an error-themed embed (alias for error_embed)
        
        Args:
            title: Embed title (default: None)
            description: Embed description (default: None)
            thumbnail: Whether to show error icon as thumbnail (default: False)
            guild: The Discord guild for customization (default: None)
            bot: The Discord bot instance for customization (default: None)
            **kwargs: Additional arguments for create_embed
            
        Returns:
            discord.Embed: Error-themed embed
        """
        return await cls.error_embed(title, description, thumbnail, guild, bot, **kwargs)
    
    @classmethod
    async def create_success_embed(cls, 
                                 title: Optional[str] = None, 
                                 description: Optional[str] = None,
                                 thumbnail: bool = False,
                                 guild: Optional[discord.Guild] = None,
                                 bot: Optional[discord.Client] = None,
                                 **kwargs) -> discord.Embed:
        """Create a success-themed embed (alias for success_embed)
        
        Args:
            title: Embed title (default: None)
            description: Embed description (default: None)
            thumbnail: Whether to show success icon as thumbnail (default: False)
            guild: The Discord guild for customization (default: None)
            bot: The Discord bot instance for customization (default: None)
            **kwargs: Additional arguments for create_embed
            
        Returns:
            discord.Embed: Success-themed embed
        """
        return await cls.success_embed(title, description, thumbnail, guild, bot, **kwargs)
    
    @classmethod
    async def create_info_embed(cls, 
                              title: Optional[str] = None, 
                              description: Optional[str] = None,
                              thumbnail: bool = False,
                              guild: Optional[discord.Guild] = None,
                              bot: Optional[discord.Client] = None,
                              **kwargs) -> discord.Embed:
        """Create an info-themed embed (alias for info_embed)
        
        Args:
            title: Embed title (default: None)
            description: Embed description (default: None)
            thumbnail: Whether to show info icon as thumbnail (default: False)
            guild: The Discord guild for customization (default: None)
            bot: The Discord bot instance for customization (default: None)
            **kwargs: Additional arguments for create_embed
            
        Returns:
            discord.Embed: Info-themed embed
        """
        return await cls.info_embed(title, description, thumbnail, guild, bot, **kwargs)
    
    @classmethod
    async def create_warning_embed(cls, 
                                 title: Optional[str] = None, 
                                 description: Optional[str] = None,
                                 thumbnail: bool = False,
                                 guild: Optional[discord.Guild] = None,
                                 bot: Optional[discord.Client] = None,
                                 **kwargs) -> discord.Embed:
        """Create a warning-themed embed (alias for warning_embed)
        
        Args:
            title: Embed title (default: None)
            description: Embed description (default: None)
            thumbnail: Whether to show warning icon as thumbnail (default: False)
            guild: The Discord guild for customization (default: None)
            bot: The Discord bot instance for customization (default: None)
            **kwargs: Additional arguments for create_embed
            
        Returns:
            discord.Embed: Warning-themed embed
        """
        return await cls.warning_embed(title, description, thumbnail, guild, bot, **kwargs)
    
    @classmethod
    async def create_base_embed(cls, 
                              title: Optional[str] = None, 
                              description: Optional[str] = None,
                              color: Optional[int] = None,
                              thumbnail: bool = False,
                              guild: Optional[discord.Guild] = None,
                              bot: Optional[discord.Client] = None,
                              **kwargs) -> discord.Embed:
        """Create a base embed with standard styling (alias for create_embed)
        
        Args:
            title: Embed title (default: None)
            description: Embed description (default: None)
            color: Embed color (default: None - uses primary color)
            thumbnail: Whether to show neutral icon as thumbnail (default: False)
            guild: The Discord guild for customization (default: None)
            bot: The Discord bot instance for customization (default: None)
            **kwargs: Additional arguments for create_embed
            
        Returns:
            discord.Embed: Base embed with standard styling
        """
        # Set color if provided is None
        if color is None:
            kwargs["color"] = cls.COLORS["primary"]
        else:
            kwargs["color"] = color
            
        # Add neutral icon as thumbnail if requested
        if thumbnail is not None and "thumbnail_url" not in kwargs:
            kwargs["thumbnail_url"] = cls.ICONS["neutral"]
            
        # Add guild and bot to kwargs if already is None present
        if "guild" in kwargs and guild is None:
            kwargs["guild"] = guild
            
        if "bot" in kwargs and bot is None:
            kwargs["bot"] = bot
            
        return await cls.create_embed(
            title=title,
            description=description,
            **kwargs
        )
    
    @classmethod
    async def player_stats_embed(cls, 
                                player_name: str, 
                                stats: Dict[str, Any], 
                                avatar_url: Optional[str] = None,
                                faction_color: Optional[int] = None,
                                guild: Optional[discord.Guild] = None,
                                bot: Optional[discord.Client] = None) -> discord.Embed:
        """Create a player statistics embed
        
        Args:
            player_name: Player name
            stats: Player statistics dictionary
            avatar_url: Player avatar URL (default: None)
            faction_color: Faction color (default: None)
            guild: The Discord guild for customization (default: None)
            bot: The Discord bot instance for customization (default: None)
            
        Returns:
            discord.Embed: Player statistics embed
        """
        # Set color based on faction or default
        color = faction_color or cls.COLORS["primary"]
        
        # Format player stats
        fields = [
            {"name": "Kills", "value": str(stats.get("kills", 0)), "inline": True},
            {"name": "Deaths", "value": str(stats.get("deaths", 0)), "inline": True},
            {"name": "K/D Ratio", "value": f"{stats.get('kd_ratio', 0.0):.2f}", "inline": True},
            {"name": "Favorite Weapon", "value": stats.get("favorite_weapon", "None"), "inline": True},
            {"name": "Longest Kill", "value": f"{stats.get('longest_kill', 0)}m", "inline": True},
            {"name": "Playtime", "value": stats.get("playtime", "0h"), "inline": True},
        ]
        
        # Add additional stats if available
        if "level" in stats:
            fields.append({"name": "Level", "value": str(stats["level"]), "inline": True})
            
        if "rank" in stats:
            fields.append({"name": "Rank", "value": f"#{stats['rank']}", "inline": True})
            
        # Create embed
        return await cls.create_embed(
            title=f"{player_name}'s Statistics",
            color=color,
            fields=fields,
            thumbnail_url=avatar_url or cls.ICONS["stats"],
            footer_text="Last updated",
            timestamp=datetime.utcnow(),
            guild=guild,
            bot=bot
        )
    
    @classmethod
    async def faction_stats_embed(cls, 
                                faction_name: str, 
                                stats: Dict[str, Any], 
                                faction_icon: Optional[str] = None,
                                faction_color: Optional[int] = None,
                                guild: Optional[discord.Guild] = None,
                                bot: Optional[discord.Client] = None) -> discord.Embed:
        """Create a faction statistics embed
        
        Args:
            faction_name: Faction name
            stats: Faction statistics dictionary
            faction_icon: Faction icon URL (default: None)
            faction_color: Faction color (default: None)
            guild: The Discord guild for customization (default: None)
            bot: The Discord bot instance for customization (default: None)
            
        Returns:
            discord.Embed: Faction statistics embed
        """
        # Set color based on faction or default
        if faction_color is not None:
            color = faction_color
        elif faction_name.lower() == "faction a":
            color = cls.COLORS["faction_a"]
        elif faction_name.lower() == "faction b":
            color = cls.COLORS["faction_b"]
        else:
            color = cls.COLORS["primary"]
        
        # Set icon based on faction or default
        if faction_icon is not None:
            icon = faction_icon
        elif faction_name.lower() == "faction a":
            icon = cls.ICONS["faction_a"]
        elif faction_name.lower() == "faction b":
            icon = cls.ICONS["faction_b"]
        else:
            icon = cls.ICONS["stats"]
        
        # Format faction stats
        fields = [
            {"name": "Members", "value": str(stats.get("members", 0)), "inline": True},
            {"name": "Total Kills", "value": str(stats.get("total_kills", 0)), "inline": True},
            {"name": "Total Deaths", "value": str(stats.get("total_deaths", 0)), "inline": True},
            {"name": "K/D Ratio", "value": f"{stats.get('kd_ratio', 0.0):.2f}", "inline": True},
            {"name": "Territory", "value": stats.get("territory", "None"), "inline": True},
            {"name": "Ranking", "value": f"#{stats.get('rank', 0)}", "inline": True},
        ]
        
        # Add top players if available
        if "top_players" in stats and stats["top_players"]:
            top_players = "\n".join([f"{i+1}. {player}" for i, player in enumerate(stats["top_players"])])
            fields.append({"name": "Top Players", "value": top_players, "inline": False})
        
        # Create embed
        return await cls.create_embed(
            title=f"{faction_name} Statistics",
            color=color,
            fields=fields,
            thumbnail_url=icon,
            footer_text="Last updated",
            timestamp=datetime.utcnow(),
            guild=guild,
            bot=bot
        )
    
    @classmethod
    async def leaderboard_embed(cls, 
                              title: str, 
                              leaderboard: List[Dict[str, Any]],
                              color: Optional[int] = None,
                              icon: Optional[str] = None,
                              guild: Optional[discord.Guild] = None,
                              bot: Optional[discord.Client] = None) -> discord.Embed:
        """Create a leaderboard embed
        
        Args:
            title: Leaderboard title
            leaderboard: List of player/faction dictionaries with name, value, and rank
            color: Embed color (default: None)
            icon: Icon URL (default: None)
            guild: The Discord guild for customization (default: None)
            bot: The Discord bot instance for customization (default: None)
            
        Returns:
            discord.Embed: Leaderboard embed
        """
        # Set default color
        color = color or cls.COLORS["gold"]
        
        # Set default icon
        icon = icon or cls.ICONS["trophy"]
        
        # Format leaderboard
        description = ""
        
        for i, entry in enumerate(leaderboard[:10]):  # Show top 10
            # Generate medal emoji for top 3
            if i == 0:
                medal = "🥇"
            elif i == 1:
                medal = "🥈"
            elif i == 2:
                medal = "🥉"
            else:
                medal = f"`{i+1}.`"
                
            # Add entry to description
            description += f"{medal} **{entry['name']}** - {entry['value']}\n"
        
        # Create embed
        return await cls.create_embed(
            title=title,
            description=description,
            color=color,
            thumbnail_url=icon,
            footer_text="Last updated",
            timestamp=datetime.utcnow(),
            guild=guild,
            bot=bot
        )
    
    @classmethod
    async def create_progress_embed(cls,
                                  title: str,
                                  description: str,
                                  progress_value: int = 0,
                                  max_value: int = 100,
                                  color: Optional[int] = None,
                                  guild: Optional[discord.Guild] = None,
                                  bot: Optional[discord.Client] = None,
                                  **kwargs) -> discord.Embed:
        """Create a progress embed with a progress indicator
        
        Args:
            title: Embed title
            description: Embed description
            progress_value: Current progress value (default: 0)
            max_value: Maximum progress value (default: 100)
            color: Embed color (default: None)
            guild: The Discord guild for customization (default: None)
            bot: The Discord bot instance for customization (default: None)
            **kwargs: Additional arguments for create_embed
            
        Returns:
            discord.Embed: Progress embed
        """
        # Set default color if not provided
        color = color or cls.COLORS["primary"]
        
        # Calculate progress percentage
        progress_pct = min(100, max(0, int((progress_value / max(1, max_value)) * 100)))
        
        # Create progress bar
        progress_bar = cls._create_progress_bar(progress_pct)
        
        # Add progress bar to description
        full_description = f"{description}\n\n{progress_bar} **{progress_pct}%**"
        
        # Set timestamp to now if not provided
        if "timestamp" not in kwargs:
            kwargs["timestamp"] = datetime.utcnow()
            
        # Create embed
        return await cls.create_embed(
            title=title,
            description=full_description,
            color=color,
            guild=guild,
            bot=bot,
            **kwargs
        )
    
    @staticmethod
    def _create_progress_bar(percent: int, length: int = 15) -> str:
        """Create a progress bar
        
        Args:
            percent: Progress percentage (0-100)
            length: Length of the progress bar (default: 15)
            
        Returns:
            str: Progress bar string
        """
        # Ensure percent is within bounds
        percent = min(100, max(0, percent))
        
        # Calculate filled and empty segments
        filled_length = int(length * percent / 100)
        empty_length = length - filled_length
        
        # Create progress bar
        filled = "█" * filled_length
        empty = "░" * empty_length
        
        return f"{filled}{empty}"
        
    @classmethod
    async def create_standard_embed(cls, 
                                  title: str,
                                  description: str,
                                  color: Optional[int] = None,
                                  guild: Optional[discord.Guild] = None,
                                  bot: Optional[discord.Client] = None,
                                  **kwargs) -> discord.Embed:
        """Create a base embed with standard formatting
        
        Args:
            title: Embed title
            description: Embed description
            color: Embed color (default: None)
            guild: The Discord guild for customization (default: None)
            bot: The Discord bot instance for customization (default: None)
            **kwargs: Additional arguments for create_embed
            
        Returns:
            discord.Embed: Base embed
        """
        # Set default color if provided is None
        color = color or cls.COLORS["primary"]
        
        # Set timestamp to now if provided is None
        if "timestamp" not in kwargs:
            kwargs["timestamp"] = datetime.utcnow()
            
        # Set default footer if provided is None
        if "footer_text" not in kwargs:
            # Get bot name to use in footer
            from utils.helpers import get_bot_name
            bot_name = "Tower of Temptation"
            if bot is not None and guild:
                bot_name = get_bot_name(bot, guild)
            
            kwargs["footer_text"] = f"Powered By {bot_name}"
        
        # Create embed
        return await cls.create_embed(
            title=title,
            description=description,
            color=color,
            guild=guild,
            bot=bot,
            **kwargs
        )
        
    @classmethod
    async def create_stats_embed(cls, 
                               player_name: str,
                               stats: Dict[str, Any],
                               avatar_url: Optional[str] = None,
                               guild: Optional[discord.Guild] = None,
                               bot: Optional[discord.Client] = None,
                               **kwargs) -> discord.Embed:
        """Create a player stats embed (alias for player_stats_embed)
        
        Args:
            player_name: Player name
            stats: Player statistics dictionary
            avatar_url: Player avatar URL (default: None)
            guild: The Discord guild for customization (default: None)
            bot: The Discord bot instance for customization (default: None)
            **kwargs: Additional arguments for player_stats_embed
            
        Returns:
            discord.Embed: Player statistics embed
        """
        # Get faction color if available
        faction_color = None
        if "faction" in stats and stats["faction"]:
            faction_name = stats["faction"].lower()
            if faction_name == "faction a":
                faction_color = cls.COLORS["faction_a"]
            elif faction_name == "faction b":
                faction_color = cls.COLORS["faction_b"]
        
        return await cls.player_stats_embed(
            player_name=player_name,
            stats=stats,
            avatar_url=avatar_url,
            faction_color=faction_color,
            guild=guild,
            bot=bot
        )
    
    @classmethod
    async def create_server_stats_embed(cls, 
                                      server_name: str,
                                      stats: Dict[str, Any],
                                      server_icon: Optional[str] = None,
                                      color: Optional[int] = None,
                                      guild: Optional[discord.Guild] = None,
                                      bot: Optional[discord.Client] = None) -> discord.Embed:
        """Create a server statistics embed
        
        Args:
            server_name: Server name
            stats: Server statistics dictionary
            server_icon: Server icon URL (default: None)
            color: Embed color (default: None)
            guild: The Discord guild for customization (default: None)
            bot: The Discord bot instance for customization (default: None)
            
        Returns:
            discord.Embed: Server statistics embed
        """
        # Set color to default if provided is None
        color = color or cls.COLORS["primary"]
        
        # Format server stats
        fields = []
        
        # Add key stats as fields
        for key, value in stats.items():
            # Format the key name nicely
            key_name = key.replace("_", " ").title()
            
            # Format the value based on type
            if isinstance(value, (int, float)):
                if key.endswith("_ratio"):
                    formatted_value = f"{value:.2f}"
                else:
                    formatted_value = f"{value:,}"
            else:
                formatted_value = str(value)
            
            fields.append({
                "name": key_name,
                "value": formatted_value,
                "inline": True
            })
        
        # Create embed
        return await cls.create_embed(
            title=f"{server_name} Statistics",
            color=color,
            fields=fields,
            thumbnail_url=server_icon or cls.ICONS["stats"],
            footer_text="Last updated",
            timestamp=datetime.utcnow(),
            guild=guild,
            bot=bot
        )
    

    
    @classmethod
    async def create_kill_embed(cls,
                              killer_name: str,
                              victim_name: str,
                              weapon: str,
                              distance: Optional[float] = None,
                              killer_faction: Optional[str] = None,
                              victim_faction: Optional[str] = None,
                              timestamp: Optional[datetime] = None,
                              guild: Optional[discord.Guild] = None,
                              bot: Optional[discord.Client] = None) -> discord.Embed:
        """Create a kill feed embed
        
        Args:
            killer_name: Name of the killer
            victim_name: Name of the victim
            weapon: Weapon used for the kill
            distance: Kill distance in meters (default: None)
            killer_faction: Faction of the killer (default: None)
            victim_faction: Faction of the victim (default: None)
            timestamp: Time of the kill (default: None)
            guild: The Discord guild for customization (default: None)
            bot: The Discord bot instance for customization (default: None)
            
        Returns:
            discord.Embed: Kill feed embed
        """
        # Determine color based on factions
        if killer_faction is not None and killer_faction.lower() == "faction a":
            color = cls.COLORS["faction_a"]
        elif killer_faction is not None and killer_faction.lower() == "faction b":
            color = cls.COLORS["faction_b"]
        else:
            color = cls.COLORS["primary"]
        
        # Create title with kill info
        title = f"{killer_name} ⚔️ {victim_name}"
        
        # Create description with weapon and distance
        description = f"**Weapon:** {weapon}"
        if distance is not None:
            description += f"\n**Distance:** {distance:.1f}m"
            
        if killer_faction is not None and victim_faction:
            description += f"\n**Factions:** {killer_faction} vs {victim_faction}"
        
        # Create embed
        return await cls.create_embed(
            title=title,
            description=description,
            color=color,
            thumbnail_url=cls.ICONS["skull"],
            footer_text="Kill Feed",
            timestamp=timestamp or datetime.utcnow(),
            guild=guild,
            bot=bot
        )
    
    @classmethod
    async def create_event_embed(cls,
                               event_name: str,
                               description: str,
                               start_time: Optional[datetime] = None,
                               end_time: Optional[datetime] = None,
                               location: Optional[str] = None,
                               rewards: Optional[str] = None,
                               thumbnail_url: Optional[str] = None,
                               guild: Optional[discord.Guild] = None,
                               bot: Optional[discord.Client] = None) -> discord.Embed:
        """Create an event announcement embed
        
        Args:
            event_name: Name of the event
            description: Event description
            start_time: Event start time (default: None)
            end_time: Event end time (default: None)
            location: Event location (default: None)
            rewards: Event rewards (default: None)
            thumbnail_url: URL for event thumbnail (default: None)
            guild: The Discord guild for customization (default: None)
            bot: The Discord bot instance for customization (default: None)
            
        Returns:
            discord.Embed: Event announcement embed
        """
        # Create fields for additional information
        fields = []
        
        if start_time is not None:
            fields.append({
                "name": "Start Time",
                "value": start_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "inline": True
            })
            
        if end_time is not None:
            fields.append({
                "name": "End Time",
                "value": end_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "inline": True
            })
            
        if location is not None:
            fields.append({
                "name": "Location",
                "value": location,
                "inline": True
            })
            
        if rewards is not None:
            fields.append({
                "name": "Rewards",
                "value": rewards,
                "inline": False
            })
        
        # Create embed
        return await cls.create_embed(
            title=f"Event: {event_name}",
            description=description,
            color=cls.COLORS["gold"],
            fields=fields,
            thumbnail_url=thumbnail_url or cls.ICONS["trophy"],
            footer_text="Tower of Temptation Events",
            timestamp=datetime.utcnow(),
            guild=guild,
            bot=bot
        )
    
    @classmethod
    async def create_error_error_embed(cls, 
                                     title: Optional[str] = None, 
                                     description: Optional[str] = None,
                                     guild: Optional[discord.Guild] = None,
                                     bot: Optional[discord.Client] = None,
                                     **kwargs) -> discord.Embed:
        """Create a critical error embed (for errors during error handling)
        
        Args:
            title: Embed title (default: "Critical Error")
            description: Embed description (default: "An error occurred while handling an error")
            guild: The Discord guild for customization (default: None)
            bot: The Discord bot instance for customization (default: None)
            **kwargs: Additional arguments for create_embed
            
        Returns:
            discord.Embed: Critical error embed
        """
        # Use provided title or default
        title = title or "Critical Error"
        
        # Use provided description or default
        description = description or "An error occurred while handling an error"
            
        # Create a simple embed with minimal dependencies
        embed = discord.Embed(
            title=title,
            description=description,
            color=0xFF0000  # Bright red for critical errors
        )
        
        # Set timestamp
        embed.timestamp = datetime.utcnow()
        
        # Set footer
        embed.set_footer(text="Critical System Error")
            
        return embed
    
    @classmethod
    async def help_embed(cls, 
                        title: str, 
                        description: str,
                        commands: List[Dict[str, str]],
                        footer_text: Optional[str] = None,
                        guild: Optional[discord.Guild] = None,
                        bot: Optional[discord.Client] = None) -> discord.Embed:
        """Create a help embed
        
        Args:
            title: Help title
            description: Help description
            commands: List of command dictionaries with name and description
            footer_text: Footer text (default: None)
            guild: The Discord guild for customization (default: None)
            bot: The Discord bot instance for customization (default: None)
            
        Returns:
            discord.Embed: Help embed
        """
        # Create fields for commands
        fields = []
        
        for cmd in commands:
            fields.append({
                "name": cmd["name"],
                "value": cmd["description"],
                "inline": False
            })
        
        # If no custom footer text is provided, use a default with bot name
        if footer_text is None and bot and guild:
            from utils.helpers import get_bot_name
            bot_name = get_bot_name(bot, guild)
            footer_text = f"Use /{bot_name} help <command> for more details"
        
        # Create embed
        return await cls.create_embed(
            title=title,
            description=description,
            color=cls.COLORS["info"],
            fields=fields,
            thumbnail_url=cls.ICONS["info"],
            footer_text=footer_text,
            timestamp=datetime.utcnow(),
            guild=guild,
            bot=bot
        )
