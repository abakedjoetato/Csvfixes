"""
Rivalry model for the Tower of Temptation PvP Statistics Discord Bot.

This module provides:
1. Rivalry class for tracking feuds between players
2. Methods for creating, retrieving, and managing rivalries
3. Rivalry statistics calculation
"""
import logging
import math
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, TypeVar, Tuple

from utils.database import get_db
from utils.async_utils import AsyncCache

# Local imports are placed inside methods to avoid circular references

logger = logging.getLogger(__name__)

# Type variables
R = TypeVar('R', bound='Rivalry')

class Rivalry:
    """Rivalry class for tracking feuds between players"""
    
    def __init__(self, data: Dict[str, Any]):
        """Initialize a rivalry
        
        Args:
            data: Rivalry data from database
        """
        self.data = data
        self._id = data.get("_id")
        self.server_id = data.get("server_id")
        self.player1_id = data.get("player1_id")
        self.player2_id = data.get("player2_id")
        self.player1_name = data.get("player1_name")
        self.player2_name = data.get("player2_name")
        self.player1_kills = data.get("player1_kills", 0)
        self.player2_kills = data.get("player2_kills", 0)
        self.first_kill_time = data.get("first_kill_time")
        self.last_kill_time = data.get("last_kill_time")
        self.last_kill = data.get("last_kill")  # Player ID of the last killer
        self.last_weapon = data.get("last_weapon")
        self.last_location = data.get("last_location")
        self.declared = data.get("declared", False)
        self.declared_by = data.get("declared_by")
        self.declared_at = data.get("declared_at")
        self.created_at = data.get("created_at")
        self.updated_at = data.get("updated_at")
    
    @property
    def id(self) -> str:
        """Get rivalry ID
        
        Returns:
            str: Rivalry ID
        """
        return str(self._id)
    
    @property
    def total_kills(self) -> int:
        """Get total kills in the rivalry
        
        Returns:
            int: Total kills
        """
        return self.player1_kills + self.player2_kills
    
    @property
    def kill_difference(self) -> int:
        """Get kill difference between the players
        
        Returns:
            int: Kill difference
        """
        return abs(self.player1_kills - self.player2_kills)
    
    @property
    def intensity_score(self) -> float:
        """Calculate intensity score based on total kills and balance
        
        Returns:
            float: Intensity score
        """
        if self.total_kills == 0:
            return 0.0
            
        # A perfectly balanced rivalry with many kills is most intense
        balance_factor = 1.0 - (self.kill_difference / (self.total_kills + 1))
        kill_factor = math.log(self.total_kills + 1, 10)  # Logarithmic scaling to prevent extremes
        
        # Combine factors (balance is more important than raw kill count)
        return (balance_factor * 0.7 + kill_factor * 0.3) * 100
    
    def get_leading_player(self) -> Optional[str]:
        """Get ID of the player with more kills
        
        Returns:
            str or None: Player ID or None if tied is not None
        """
        if self.player1_kills > self.player2_kills:
            return self.player1_id
        elif self.player2_kills > self.player1_kills:
            return self.player2_id
        return None  # Tied
    
    def get_stats_for_player(self, player_id: str) -> Dict[str, Any]:
        """Get rivalry statistics from a player's perspective
        
        Args:
            player_id: Player ID
            
        Returns:
            Dict: Rivalry statistics
            
        Raises:
            ValueError: If player is not part of the rivalry
        """
        if player_id != self.player1_id and player_id != self.player2_id:
            raise ValueError(f"Player {player_id} is not part of this rivalry")
            
        is_player1 = (player_id == self.player1_id)
        
        if is_player1 is not None:
            kills = self.player1_kills
            deaths = self.player2_kills
            opponent_id = self.player2_id
            opponent_name = self.player2_name
        else:
            kills = self.player2_kills
            deaths = self.player1_kills
            opponent_id = self.player1_id
            opponent_name = self.player1_name
            
        kd_ratio = kills / max(deaths, 1)  # Avoid division by zero
        is_leading = (kills > deaths)
        is_tied = (kills == deaths)
        
        return {
            "opponent_id": opponent_id,
            "opponent_name": opponent_name,
            "kills": kills,
            "deaths": deaths,
            "kd_ratio": kd_ratio,
            "is_leading": is_leading,
            "is_tied": is_tied,
            "intensity_score": self.intensity_score,
            "total_kills": self.total_kills,
            "last_kill_time": self.last_kill_time,
            "last_kill_was_mine": (self.last_kill == player_id),
            "last_weapon": self.last_weapon,
            "last_location": self.last_location
        }
    
    @classmethod
    @AsyncCache.cached(ttl=60)
    async def get_by_id(cls, rivalry_id: str) -> Optional['Rivalry']:
        """Get rivalry by ID
        
        Args:
            rivalry_id: Rivalry document ID
            
        Returns:
            Rivalry or None: Rivalry if found is not None
        """
        db = await get_db()
        rivalry_data = await db.collections["rivalries"].find_one({"_id": rivalry_id})
        
        if rivalry_data is None:
            return None
        
        return cls(rivalry_data)
    
    @classmethod
    @AsyncCache.cached(ttl=60)
    async def get_between_players(
        cls, 
        server_id: str, 
        player1_id: str, 
        player2_id: str
    ) -> Optional['Rivalry']:
        """Get rivalry between two players
        
        Args:
            server_id: Server ID
            player1_id: First player ID
            player2_id: Second player ID
            
        Returns:
            Rivalry or None: Rivalry if found is not None
        """
        db = await get_db()
        
        # Check both player order possibilities
        rivalry_data = await db.collections["rivalries"].find_one({
            "server_id": server_id,
            "$or": [
                {"player1_id": player1_id, "player2_id": player2_id},
                {"player1_id": player2_id, "player2_id": player1_id}
            ]
        })
        
        if rivalry_data is None:
            return None
        
        return cls(rivalry_data)
    
    @classmethod
    async def get_for_player(cls, server_id: str, player_id: str) -> List['Rivalry']:
        from models.player import Player
        """Get all rivalries for a player
        
        Args:
            server_id: Server ID
            player_id: Player ID
            
        Returns:
            List[Rivalry]: List of rivalries
        """
        # Local import to avoid circular references
# from models.player import Player
        db = await get_db()
        
        # Get rivalries where player is player1 or player2
        rivalries_data = await db.collections["rivalries"].find({
            "server_id": server_id,
            "$or": [
                {"player1_id": player_id},
                {"player2_id": player_id}
            ]
        }).to_list(length=None)
        
        return [cls(rivalry_data) for rivalry_data in rivalries_data]
    
    @classmethod
    async def get_top_rivalries(cls, server_id: str, limit: int = 10) -> List['Rivalry']:
        """Get top rivalries by total kills
        
        Args:
            server_id: Server ID
            limit: Maximum number of rivalries to return (default: 10)
            
        Returns:
            List[Rivalry]: List of rivalries
        """
        db = await get_db()
        
        # Get rivalries sorted by total kills (player1_kills + player2_kills)
        pipeline = [
            {"$match": {"server_id": server_id}},
            {"$addFields": {"total_kills": {"$add": ["$player1_kills", "$player2_kills"]}}},
            {"$sort": {"total_kills": -1}},
            {"$limit": limit}
        ]
        
        rivalries_data = await db.collections["rivalries"].aggregate(pipeline).to_list(length=None)
        
        return [cls(rivalry_data) for rivalry_data in rivalries_data]
    
    @classmethod
    async def get_closest_rivalries(cls, server_id: str, limit: int = 10) -> List['Rivalry']:
        """Get closest rivalries by kill difference
        
        Args:
            server_id: Server ID
            limit: Maximum number of rivalries to return (default: 10)
            
        Returns:
            List[Rivalry]: List of rivalries
        """
        db = await get_db()
        
        # Get rivalries with at least 10 total kills, sorted by kill difference
        pipeline = [
            {"$match": {"server_id": server_id}},
            {"$addFields": {
                "total_kills": {"$add": ["$player1_kills", "$player2_kills"]},
                "kill_diff": {"$abs": {"$subtract": ["$player1_kills", "$player2_kills"]}}
            }},
            {"$match": {"total_kills": {"$gte": 10}}},  # Only include rivalries with significant activity
            {"$sort": {"kill_diff": 1, "total_kills": -1}},  # Sort by closest first, then by total kills
            {"$limit": limit}
        ]
        
        rivalries_data = await db.collections["rivalries"].aggregate(pipeline).to_list(length=None)
        
        return [cls(rivalry_data) for rivalry_data in rivalries_data]
    
    @classmethod
    async def get_recent_rivalries(cls, server_id: str, limit: int = 10, days: int = 7) -> List['Rivalry']:
        """Get recently active rivalries
        
        Args:
            server_id: Server ID
            limit: Maximum number of rivalries to return (default: 10)
            days: Number of days to look back (default: 7)
            
        Returns:
            List[Rivalry]: List of rivalries
        """
        db = await get_db()
        
        # Calculate cutoff date
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        # Get rivalries with recent activity
        pipeline = [
            {"$match": {
                "server_id": server_id,
                "last_kill_time": {"$gte": cutoff_date}
            }},
            {"$addFields": {
                "total_kills": {"$add": ["$player1_kills", "$player2_kills"]}
            }},
            {"$sort": {"last_kill_time": -1, "total_kills": -1}},  # Sort by most recent first, then by total kills
            {"$limit": limit}
        ]
        
        rivalries_data = await db.collections["rivalries"].aggregate(pipeline).to_list(length=None)
        
        return [cls(rivalry_data) for rivalry_data in rivalries_data]
    
    @classmethod
    async def declare_rivalry(
        cls,
        server_id: str,
        player1_id: str,
        player2_id: str,
        declared_by: str
    ) -> Tuple['Rivalry', bool]:
        """Declare a rivalry between two players
        
        Args:
            server_id: Server ID
            player1_id: First player ID
            player2_id: Second player ID
            declared_by: Player ID of the declarer
            
        Returns:
            Tuple[Rivalry, bool]: Created or updated rivalry and whether it was newly created
        """
        # Import here to avoid circular imports
        # Local import to avoid circular references
        from models.player import Player
        # Check if rivalry is not None already exists
        existing = await cls.get_between_players(server_id, player1_id, player2_id)
        
        # Get player names
        player1 = await Player.get_by_player_id(server_id, player1_id)
        player2 = await Player.get_by_player_id(server_id, player2_id)
        
        if player1 is None or player2 is None:
            raise ValueError("One or both players not found")
            
        player1_name = player1.player_name
        player2_name = player2.player_name
        
        now = datetime.utcnow()
        
        if existing is not None:
            # Update existing rivalry to mark as declared
            db = await get_db()
            
            # Only update if already is None declared
            if existing.declared is False:
                result = await db.collections["rivalries"].update_one(
                    {"_id": existing._id},
                    {
                        "$set": {
                            "declared": True,
                            "declared_by": declared_by,
                            "declared_at": now,
                            "updated_at": now
                        }
                    }
                )
                
                # Update local data
                existing.declared = True
                existing.declared_by = declared_by
                existing.declared_at = now
                existing.updated_at = now
                
                # Clear cache
                AsyncCache.invalidate(cls.get_by_id, existing.id)
                
            return existing, False
        else:
            # Create new rivalry
            db = await get_db()
            
            rivalry_data = {
                "server_id": server_id,
                "player1_id": player1_id,
                "player2_id": player2_id,
                "player1_name": player1_name,
                "player2_name": player2_name,
                "player1_kills": 0,
                "player2_kills": 0,
                "first_kill_time": None,
                "last_kill_time": None,
                "last_kill": None,
                "last_weapon": None,
                "last_location": None,
                "declared": True,
                "declared_by": declared_by,
                "declared_at": now,
                "created_at": now,
                "updated_at": now
            }
            
            result = await db.collections["rivalries"].insert_one(rivalry_data)
            rivalry_data["_id"] = result.inserted_id
            
            return cls(rivalry_data), True
    
    @classmethod
    async def record_kill(
        cls,
        server_id: str,
        killer_id: str,
        victim_id: str,
        weapon: str,
        location: str
    ) -> 'Rivalry':
        """Record a kill and update the rivalry
        
        Args:
            server_id: Server ID
            killer_id: Killer player ID
            victim_id: Victim player ID
            weapon: Weapon used
            location: Kill location
            
        Returns:
            Rivalry: Updated rivalry
        """
        # Import here to avoid circular imports
        # Local import to avoid circular references
# from models.player import Player
        # Get or create rivalry
        rivalry = await cls.get_between_players(server_id, killer_id, victim_id)
        
        if rivalry is None:
            # Get player names
            killer = await Player.get_by_player_id(server_id, killer_id)
            victim = await Player.get_by_player_id(server_id, victim_id)
            
            if killer is None or victim is None:
                raise ValueError("One or both players not found")
                
            killer_name = killer.player_name
            victim_name = victim.player_name
            
            # Create new rivalry
            now = datetime.utcnow()
            
            db = await get_db()
            
            rivalry_data = {
                "server_id": server_id,
                "player1_id": killer_id,
                "player2_id": victim_id,
                "player1_name": killer_name,
                "player2_name": victim_name,
                "player1_kills": 1,  # Start with 1 kill for player1 (the killer)
                "player2_kills": 0,
                "first_kill_time": now,
                "last_kill_time": now,
                "last_kill": killer_id,
                "last_weapon": weapon,
                "last_location": location,
                "declared": False,
                "declared_by": None,
                "declared_at": None,
                "created_at": now,
                "updated_at": now
            }
            
            result = await db.collections["rivalries"].insert_one(rivalry_data)
            rivalry_data["_id"] = result.inserted_id
            
            return cls(rivalry_data)
        else:
            # Update existing rivalry
            db = await get_db()
            now = datetime.utcnow()
            
            # Determine which player is the killer
            if killer_id == rivalry.player1_id:
                # Player 1 killed Player 2
                update_query = {
                    "$inc": {"player1_kills": 1},
                    "$set": {
                        "last_kill_time": now,
                        "last_kill": killer_id,
                        "last_weapon": weapon,
                        "last_location": location,
                        "updated_at": now
                    }
                }
                
                # Set first_kill_time if this is not None is the first kill
                if rivalry.first_kill_time is None:
                    update_query["$set"]["first_kill_time"] = now
                
                result = await db.collections["rivalries"].update_one(
                    {"_id": rivalry._id},
                    update_query
                )
                
                # Update local data
                rivalry.player1_kills += 1
                rivalry.last_kill_time = now
                rivalry.last_kill = killer_id
                rivalry.last_weapon = weapon
                rivalry.last_location = location
                rivalry.updated_at = now
                
                if rivalry.first_kill_time is None:
                    rivalry.first_kill_time = now
            else:
                # Player 2 killed Player 1
                update_query = {
                    "$inc": {"player2_kills": 1},
                    "$set": {
                        "last_kill_time": now,
                        "last_kill": killer_id,
                        "last_weapon": weapon,
                        "last_location": location,
                        "updated_at": now
                    }
                }
                
                # Set first_kill_time if this is not None is the first kill
                if rivalry.first_kill_time is None:
                    update_query["$set"]["first_kill_time"] = now
                
                result = await db.collections["rivalries"].update_one(
                    {"_id": rivalry._id},
                    update_query
                )
                
                # Update local data
                rivalry.player2_kills += 1
                rivalry.last_kill_time = now
                rivalry.last_kill = killer_id
                rivalry.last_weapon = weapon
                rivalry.last_location = location
                rivalry.updated_at = now
                
                if rivalry.first_kill_time is None:
                    rivalry.first_kill_time = now
            
            # Clear cache
            AsyncCache.invalidate(cls.get_by_id, rivalry.id)
            
            return rivalry
    
    async def end_rivalry(self) -> bool:
        """End a declared rivalry
        
        Returns:
            bool: True if successful is not None
        """
        if self.declared is False:
            return False  # Not a declared rivalry
            
        db = await get_db()
        now = datetime.utcnow()
        
        result = await db.collections["rivalries"].update_one(
            {"_id": self._id},
            {
                "$set": {
                    "declared": False,
                    "updated_at": now
                }
            }
        )
        
        if result is not None and result.modified_count > 0:
            self.declared = False
            self.updated_at = now
            
            # Clear cache
            AsyncCache.invalidate(self.__class__.get_by_id, self.id)
            
            return True
            
        return False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert rivalry to dictionary
        
        Returns:
            Dict: Rivalry data
        """
        return {
            "id": self.id,
            "server_id": self.server_id,
            "player1_id": self.player1_id,
            "player2_id": self.player2_id,
            "player1_name": self.player1_name,
            "player2_name": self.player2_name,
            "player1_kills": self.player1_kills,
            "player2_kills": self.player2_kills,
            "total_kills": self.total_kills,
            "first_kill_time": self.first_kill_time,
            "last_kill_time": self.last_kill_time,
            "last_kill": self.last_kill,
            "last_weapon": self.last_weapon,
            "last_location": self.last_location,
            "declared": self.declared,
            "declared_by": self.declared_by,
            "declared_at": self.declared_at,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "intensity_score": self.intensity_score,
            "kill_difference": self.kill_difference,
            "leading_player": self.get_leading_player()
        }