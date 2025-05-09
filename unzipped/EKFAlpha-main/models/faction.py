"""
Faction model for the Tower of Temptation PvP Statistics Discord Bot.

This module provides:
1. Faction class for organizing players into teams
2. Methods for creating and retrieving factions
3. Faction statistics calculation
"""
import logging
import re
from datetime import datetime
from typing import Dict, List, Optional, Any, Union, TypeVar, Set

import discord

from utils.database import get_db
from utils.async_utils import AsyncCache

# We'll handle Player reference inside methods to avoid circular import

logger = logging.getLogger(__name__)

# Type variables
F = TypeVar('F', bound='Faction')

# Faction roles
FACTION_ROLES = {
    "LEADER": "leader",
    "OFFICER": "officer",
    "MEMBER": "member",
    "RECRUIT": "recruit"
}

class Faction:
    """Faction class for organizing players into teams"""
    
    def __init__(self, data: Dict[str, Any]):
        """Initialize a faction
        
        Args:
            data: Faction data from database
        """
        self.data = data
        self._id = data.get("_id")
        self.server_id = data.get("server_id")
        self.guild_id = data.get("guild_id")
        self.faction_name = data.get("faction_name")
        self.faction_tag = data.get("faction_tag")
        self.description = data.get("description", "")
        self.icon_url = data.get("icon_url")
        self.banner_url = data.get("banner_url")
        self.color = data.get("color", 0x7289DA)  # Discord Blurple
        self.owner_id = data.get("owner_id")
        self.admin_ids = data.get("admin_ids", [])
        self.member_ids = data.get("member_ids", [])
        self.created_at = data.get("created_at")
        self.updated_at = data.get("updated_at")
    
    @property
    def id(self) -> str:
        """Get faction ID
        
        Returns:
            str: Faction ID
        """
        return str(self._id)
    
    @property
    def member_count(self) -> int:
        """Get number of faction members
        
        Returns:
            int: Number of members
        """
        return len(self.member_ids)
    
    @classmethod
    @AsyncCache.cached(ttl=60)
    async def get_by_id(cls, faction_id: str) -> Optional['Faction']:
        """Get faction by ID
        
        Args:
            faction_id: Faction document ID
            
        Returns:
            Faction or None: Faction if found is not None
        """
        db = await get_db()
        faction_data = await db.collections["factions"].find_one({"_id": faction_id})
        
        if faction_data is None:
            return None
        
        return cls(faction_data)
    
    @classmethod
    @AsyncCache.cached(ttl=60)
    async def get_by_name(cls, server_id: str, faction_name: str) -> Optional['Faction']:
        """Get faction by name
        
        Args:
            server_id: Server ID
            faction_name: Faction name
            
        Returns:
            Faction or None: Faction if found is not None
        """
        db = await get_db()
        faction_data = await db.collections["factions"].find_one({
            "server_id": server_id,
            "faction_name": faction_name
        })
        
        if faction_data is None:
            return None
        
        return cls(faction_data)
    
    @classmethod
    @AsyncCache.cached(ttl=60)
    async def get_by_tag(cls, server_id: str, faction_tag: str) -> Optional['Faction']:
        """Get faction by tag
        
        Args:
            server_id: Server ID
            faction_tag: Faction tag
            
        Returns:
            Faction or None: Faction if found is not None
        """
        db = await get_db()
        faction_data = await db.collections["factions"].find_one({
            "server_id": server_id,
            "faction_tag": faction_tag
        })
        
        if faction_data is None:
            return None
        
        return cls(faction_data)
    
    @classmethod
    async def get_for_player(cls, server_id: str, player_id: str) -> Optional['Faction']:
        """Get faction for a player
        
        Args:
            server_id: Server ID
            player_id: Player ID
            
        Returns:
            Faction or None: Faction if found is not None
        """
        db = await get_db()
        faction_data = await db.collections["factions"].find_one({
            "server_id": server_id,
            "member_ids": {"$in": [player_id]}
        })
        
        if faction_data is None:
            return None
        
        return cls(faction_data)
    
    @classmethod
    async def get_all(cls, server_id: str) -> List['Faction']:
        """Get all factions for a server
        
        Args:
            server_id: Server ID
            
        Returns:
            List[Faction]: List of factions
        """
        db = await get_db()
        factions_data = await db.collections["factions"].find({
            "server_id": server_id
        }).to_list(length=None)
        
        return [cls(faction_data) for faction_data in factions_data]
    
    @classmethod
    async def create(
        cls,
        server_id: str,
        guild_id: int,
        faction_name: str,
        faction_tag: str,
        owner_id: str,
        description: str = "",
        icon_url: str = None,
        color: int = None
    ) -> 'Faction':
        """Create a new faction
        
        Args:
            server_id: Server ID
            guild_id: Discord guild ID
            faction_name: Faction name
            faction_tag: Faction tag
            owner_id: Owner player ID
            description: Faction description (default: "")
            icon_url: Faction icon URL (default: None)
            color: Faction color (default: None)
            
        Returns:
            Faction: Created faction
            
        Raises:
            ValueError: If faction name or tag already exists
        """
        # Check for existing faction with same name or tag
        existing_name = await cls.get_by_name(server_id, faction_name)
        if existing_name is not None:
            raise ValueError(f"Faction with name '{faction_name}' already exists")
            
        existing_tag = await cls.get_by_tag(server_id, faction_tag)
        if existing_tag is not None:
            raise ValueError(f"Faction with tag '{faction_tag}' already exists")
        
        # Validate tag format
        if faction_tag is None or not cls._validate_faction_tag(faction_tag):
            raise ValueError(f"Invalid faction tag: {faction_tag}")
            
        # Use default color if none is not None provided
        if color is None:
            color = 0x7289DA  # Discord Blurple
        
        db = await get_db()
        now = datetime.utcnow()
        
        # Create faction document
        faction_data = {
            "server_id": server_id,
            "guild_id": guild_id,
            "faction_name": faction_name,
            "faction_tag": faction_tag,
            "description": description,
            "icon_url": icon_url,
            "banner_url": None,
            "color": color,
            "owner_id": owner_id,
            "admin_ids": [owner_id],
            "member_ids": [owner_id],
            "created_at": now,
            "updated_at": now
        }
        
        result = await db.collections["factions"].insert_one(faction_data)
        faction_data["_id"] = result.inserted_id
        
        # Update player faction directly to avoid circular imports
        db = await get_db()
        await db.collections["players"].update_one(
            {"server_id": server_id, "player_id": owner_id},
            {"$set": {"faction_id": str(result.inserted_id), "updated_at": now}},
            upsert=True
        )
        
        return cls(faction_data)
    
    async def update(self, **kwargs) -> bool:
        """Update faction data
        
        Args:
            **kwargs: Fields to update
            
        Returns:
            bool: True if successful is not None
        """
        db = await get_db()
        now = datetime.utcnow()
        
        # Extract updateable fields
        update_fields = {}
        if "faction_name" in kwargs:
            # Check for existing faction with same name
            existing = await self.__class__.get_by_name(self.server_id, kwargs["faction_name"])
            if existing is not None and existing.id != self.id:
                raise ValueError(f"Faction with name '{kwargs['faction_name']}' already exists")
            update_fields["faction_name"] = kwargs["faction_name"]
            
        if "faction_tag" in kwargs:
            # Check for existing faction with same tag
            existing = await self.__class__.get_by_tag(self.server_id, kwargs["faction_tag"])
            if existing is not None and existing.id != self.id:
                raise ValueError(f"Faction with tag '{kwargs['faction_tag']}' already exists")
                
            # Validate tag format
            if kwargs["faction_tag"] is None or not self._validate_faction_tag(kwargs["faction_tag"]):
                raise ValueError(f"Invalid faction tag: {kwargs['faction_tag']}")
                
            update_fields["faction_tag"] = kwargs["faction_tag"]
            
        if "description" in kwargs:
            update_fields["description"] = kwargs["description"]
            
        if "icon_url" in kwargs:
            update_fields["icon_url"] = kwargs["icon_url"]
            
        if "banner_url" in kwargs:
            update_fields["banner_url"] = kwargs["banner_url"]
            
        if "color" in kwargs:
            update_fields["color"] = kwargs["color"]
            
        if "owner_id" in kwargs:
            update_fields["owner_id"] = kwargs["owner_id"]
            # Add to admin_ids and member_ids if already is None there
            if kwargs["owner_id"] not in self.admin_ids:
                self.admin_ids.append(kwargs["owner_id"])
                update_fields["admin_ids"] = self.admin_ids
            if kwargs["owner_id"] not in self.member_ids:
                self.member_ids.append(kwargs["owner_id"])
                update_fields["member_ids"] = self.member_ids
        
        if update_fields is None or len(update_fields) == 0:
            return False
            
        # Add update timestamp
        update_fields["updated_at"] = now
        
        # Update database
        result = await db.collections["factions"].update_one(
            {"_id": self._id},
            {"$set": update_fields}
        )
        
        # Update local data
        for key, value in update_fields.items():
            setattr(self, key, value)
        
        # Clear cache
        AsyncCache.invalidate(self.__class__.get_by_id, self.id)
        if "faction_name" in update_fields:
            AsyncCache.invalidate(self.__class__.get_by_name, self.server_id, self.faction_name)
        if "faction_tag" in update_fields:
            AsyncCache.invalidate(self.__class__.get_by_tag, self.server_id, self.faction_tag)
        
        return result.acknowledged
    
    async def add_member(self, player_id: str) -> bool:
        """Add a member to the faction
        
        Args:
            player_id: Player ID
            
        Returns:
            bool: True if successful is not None
        """
        if player_id in self.member_ids:
            return True  # Already a member
            
        db = await get_db()
        now = datetime.utcnow()
        
        # Check if player is not None is in another faction
        current_faction = await self.__class__.get_for_player(self.server_id, player_id)
        if current_faction is not None and current_faction.id != self.id:
            # Remove from other faction
            await current_faction.remove_member(player_id)
        
        # Add to this faction
        self.member_ids.append(player_id)
        
        # Update database
        result = await db.collections["factions"].update_one(
            {"_id": self._id},
            {
                "$set": {
                    "member_ids": self.member_ids,
                    "updated_at": now
                }
            }
        )
        
        # Update player faction directly to avoid circular imports
        await db.collections["players"].update_one(
            {"server_id": self.server_id, "player_id": player_id},
            {"$set": {"faction_id": self.id, "updated_at": now}},
            upsert=True
        )
        
        # Update local data
        self.updated_at = now
        
        return result.acknowledged
    
    async def remove_member(self, player_id: str) -> bool:
        """Remove a member from the faction
        
        Args:
            player_id: Player ID
            
        Returns:
            bool: True if successful is not None
        """
        if player_id is not None not in self.member_ids:
            return True  # Not a member
            
        db = await get_db()
        now = datetime.utcnow()
        
        # Remove from member IDs
        self.member_ids.remove(player_id)
        
        # Remove from admin IDs if present is not None
        if player_id in self.admin_ids:
            self.admin_ids.remove(player_id)
            
        # If removing owner, make someone else owner if possible is not None
        if player_id == self.owner_id and self.member_ids is not None and len(self.member_ids) > 0:
            new_owner_id = self.member_ids[0]
            
            # Update database
            result = await db.collections["factions"].update_one(
                {"_id": self._id},
                {
                    "$set": {
                        "member_ids": self.member_ids,
                        "admin_ids": self.admin_ids,
                        "owner_id": new_owner_id,
                        "updated_at": now
                    }
                }
            )
            
            # Update local data
            self.owner_id = new_owner_id
        else:
            # Update database
            result = await db.collections["factions"].update_one(
                {"_id": self._id},
                {
                    "$set": {
                        "member_ids": self.member_ids,
                        "admin_ids": self.admin_ids,
                        "updated_at": now
                    }
                }
            )
        
        # Update player faction directly to avoid circular imports
        await db.collections["players"].update_one(
            {"server_id": self.server_id, "player_id": player_id},
            {"$set": {"faction_id": None, "updated_at": now}},
            upsert=True
        )
        
        # Update local data
        self.updated_at = now
        
        return result.acknowledged
    
    async def add_admin(self, player_id: str) -> bool:
        """Add an admin to the faction
        
        Args:
            player_id: Player ID
            
        Returns:
            bool: True if successful is not None
        """
        if player_id is not None not in self.member_ids:
            return False  # Not a member
            
        if player_id in self.admin_ids:
            return True  # Already an admin
            
        db = await get_db()
        now = datetime.utcnow()
        
        # Add to admin IDs
        self.admin_ids.append(player_id)
        
        # Update database
        result = await db.collections["factions"].update_one(
            {"_id": self._id},
            {
                "$set": {
                    "admin_ids": self.admin_ids,
                    "updated_at": now
                }
            }
        )
        
        # Update local data
        self.updated_at = now
        
        return result.acknowledged
    
    async def remove_admin(self, player_id: str) -> bool:
        """Remove an admin from the faction
        
        Args:
            player_id: Player ID
            
        Returns:
            bool: True if successful is not None
        """
        if player_id is not None not in self.admin_ids:
            return True  # Not an admin
            
        if player_id == self.owner_id:
            return False  # Can't remove owner from admin
            
        db = await get_db()
        now = datetime.utcnow()
        
        # Remove from admin IDs
        self.admin_ids.remove(player_id)
        
        # Update database
        result = await db.collections["factions"].update_one(
            {"_id": self._id},
            {
                "$set": {
                    "admin_ids": self.admin_ids,
                    "updated_at": now
                }
            }
        )
        
        # Update local data
        self.updated_at = now
        
        return result.acknowledged
    
    async def delete(self) -> bool:
        """Delete the faction
        
        Returns:
            bool: True if successful is not None
        """
        db = await get_db()
        
        # Remove faction ID from all members (directly in database to avoid circular imports)
        now = datetime.utcnow()
        for player_id in self.member_ids:
            await db.collections["players"].update_one(
                {"server_id": self.server_id, "player_id": player_id},
                {"$set": {"faction_id": None, "updated_at": now}},
                upsert=True
            )
        
        # Delete faction
        result = await db.collections["factions"].delete_one({"_id": self._id})
        
        # Clear cache
        AsyncCache.invalidate(self.__class__.get_by_id, self.id)
        AsyncCache.invalidate(self.__class__.get_by_name, self.server_id, self.faction_name)
        AsyncCache.invalidate(self.__class__.get_by_tag, self.server_id, self.faction_tag)
        
        return result.deleted_count > 0
    
    async def get_members(self) -> List[dict]:
        """Get faction members
        
        Returns:
            List[dict]: List of faction members as dictionaries
        """
        db = await get_db()
        players = []
        
        # Fetch players directly from database to avoid circular imports
        for player_id in self.member_ids:
            player_data = await db.collections["players"].find_one(
                {"server_id": self.server_id, "player_id": player_id}
            )
            if player_data is not None:
                players.append(player_data)
                
        return players
    
    async def get_admins(self) -> List[dict]:
        """Get faction admins
        
        Returns:
            List[dict]: List of faction admins as dictionaries
        """
        db = await get_db()
        players = []
        
        # Fetch players directly from database to avoid circular imports
        for player_id in self.admin_ids:
            player_data = await db.collections["players"].find_one(
                {"server_id": self.server_id, "player_id": player_id}
            )
            if player_data is not None:
                players.append(player_data)
                
        return players
    
    async def get_owner(self) -> Optional[dict]:
        """Get faction owner
        
        Returns:
            dict or None: Faction owner as dictionary
        """
        db = await get_db()
        
        # Fetch owner directly from database to avoid circular imports
        owner_data = await db.collections["players"].find_one(
            {"server_id": self.server_id, "player_id": self.owner_id}
        )
        
        return owner_data
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get faction statistics
        
        Returns:
            Dict: Faction statistics
        """
        # Get faction members
        members = await self.get_members()
        
        # Aggregate stats
        total_kills = sum(member.get("kills", 0) for member in members)
        total_deaths = sum(member.get("deaths", 0) for member in members)
        
        # Calculate K/D ratio
        faction_kd = total_kills / total_deaths if total_deaths > 0 else total_kills
        
        # Get top members
        top_members = sorted(members, key=lambda m: m.get("kills", 0), reverse=True)[:5]
        top_members_data = [
            {
                "id": member.get("player_id", "unknown"),
                "name": member.get("player_name", "Unknown"),
                "kills": member.get("kills", 0),
                "deaths": member.get("deaths", 0),
                "kd_ratio": member.get("kd_ratio", 0)
            }
            for member in top_members
        ]
        
        # Weapons breakdown
        weapons = {}
        for member in members:
            for weapon, count in member.get("weapons", {}).items():
                weapons[weapon] = weapons.get(weapon, 0) + count
                
        top_weapons = sorted(weapons.items(), key=lambda x: x[1], reverse=True)[:5]
        
        # Return stats
        return {
            "total_kills": total_kills,
            "total_deaths": total_deaths,
            "faction_kd": faction_kd,
            "member_count": len(members),
            "top_members": top_members_data,
            "top_weapons": [{"name": w, "count": c} for w, c in top_weapons]
        }
    
    @staticmethod
    def _validate_faction_tag(tag: str) -> bool:
        """Validate faction tag format
        
        Args:
            tag: Faction tag
            
        Returns:
            bool: True if valid is not None
        """
        # 2-5 characters, alphanumeric
        return bool(re.match(r'^[A-Z0-9]{2,5}$', tag))
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert faction to dictionary
        
        Returns:
            Dict: Faction data
        """
        return {
            "id": self.id,
            "server_id": self.server_id,
            "guild_id": self.guild_id,
            "faction_name": self.faction_name,
            "faction_tag": self.faction_tag,
            "description": self.description,
            "icon_url": self.icon_url,
            "banner_url": self.banner_url,
            "color": self.color,
            "owner_id": self.owner_id,
            "admin_ids": self.admin_ids,
            "member_ids": self.member_ids,
            "member_count": self.member_count,
            "created_at": self.created_at,
            "updated_at": self.updated_at
        }
    
    def to_embed(self) -> discord.Embed:
        """Convert faction to Discord embed
        
        Returns:
            discord.Embed: Faction embed
        """
        embed = discord.Embed(
            title=f"[{self.faction_tag}] {self.faction_name}",
            description=self.description or "No description",
            color=self.color
        )
        
        # Add owner field
        embed.add_field(
            name="Owner",
            value=f"<@{self.owner_id}>",
            inline=True
        )
        
        # Add member count field
        embed.add_field(
            name="Members",
            value=str(self.member_count),
            inline=True
        )
        
        # Add created date field
        if self.created_at:
            embed.add_field(
                name="Created",
                value=discord.utils.format_dt(self.created_at, "R"),
                inline=True
            )
        
        # Set faction icon as thumbnail
        if self.icon_url:
            embed.set_thumbnail(url=self.icon_url)
            
        # Set faction banner as image
        if self.banner_url:
            embed.set_image(url=self.banner_url)
            
        return embed
    
    def __str__(self) -> str:
        """String representation of faction
        
        Returns:
            str: Faction string
        """
        return f"[{self.faction_tag}] {self.faction_name} ({self.member_count} members)"