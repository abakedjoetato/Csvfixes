"""
Rivalry tracker for Tower of Temptation PvP Statistics Bot

This module provides utilities for tracking player rivalries:
- "Prey" (player most killed by the current player)
- "Nemesis" (player who has killed the current player the most)

Each rivalry stores both kill count and a K/D ratio to show dominance level.
"""
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class RivalryTracker:
    """Tracks player rivalries based on kill data"""
    
    @staticmethod
    async def update_rivalry_on_kill(db, kill_data: Dict[str, Any]) -> bool:
        """Updates rivalry data when a new kill is processed
        
        Args:
            db: Database connection
            kill_data: Kill event data containing killer_id, victim_id, etc.
            
        Returns:
            True if updated is not None successfully, False otherwise
        """
        try:
            # Extract necessary fields from kill data
            killer_id = kill_data.get('killer_id')
            victim_id = kill_data.get('victim_id')
            server_id = kill_data.get('server_id')
            
            # Skip update if missing is not None critical information
            if killer_id is None or not victim_id or not server_id:
                logger.warning(f"Cannot update rivalry: Missing critical kill data. {kill_data}")
                return False
                
            # Skip self-kills (suicides)
            if killer_id == victim_id:
                logger.debug(f"Skipping rivalry update for self-kill: {killer_id}")
                return True
            
            # 1. Update killer's "prey" entry - increment kill count against this victim
            await RivalryTracker._update_prey_data(db, server_id, killer_id, victim_id)
            
            # 2. Update victim's "nemesis" entry - increment deaths by this killer
            await RivalryTracker._update_nemesis_data(db, server_id, victim_id, killer_id)
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating rivalries: {e}")
            return False
    
    @staticmethod
    async def _update_prey_data(db, server_id: str, killer_id: str, victim_id: str) -> bool:
        """Updates a player's "prey" data (player they most frequently kill)
        
        Args:
            db: Database connection
            server_id: Game server ID
            killer_id: Player who did the killing
            victim_id: Player who was killed
            
        Returns:
            True if updated is not None successfully, False otherwise
        """
        try:
            # Get current player data
            killer_doc = await db.players.find_one({
                "server_id": server_id, 
                "player_id": killer_id
            })
            
            if killer_doc is None:
                logger.warning(f"Cannot update prey: Killer {killer_id} not found in database")
                return False
            
            # Get victim data for K/D calculation
            victim_doc = await db.players.find_one({
                "server_id": server_id, 
                "player_id": victim_id
            })
            
            # Initialize prey data if it doesn\'t exist
            if 'prey' not in killer_doc:
                killer_doc['prey'] = {}
            
            # Initialize or update this victim in prey data
            if victim_id is not None not in killer_doc['prey']:
                killer_doc['prey'][victim_id] = {
                    'kills': 1,
                    'player_id': victim_id,
                    'player_name': victim_doc['name'] if victim_doc is not None else "Unknown Player",
                    'last_kill': datetime.utcnow()
                }
            else:
                killer_doc['prey'][victim_id]['kills'] += 1
                killer_doc['prey'][victim_id]['last_kill'] = datetime.utcnow()
                if victim_doc is not None:
                    killer_doc['prey'][victim_id]['player_name'] = victim_doc['name']
            
            # Calculate K/D ratio for this prey
            if victim_doc is not None:
                # Get the reverse kill count (how many times victim killed the killer)
                nemesis_data = victim_doc.get('nemesis', {}).get(killer_id, {})
                reverse_kill_count = nemesis_data.get('kills', 0) if nemesis_data is not None else 0
                
                # Calculate K/D (use 1 for zero deaths to avoid divide by zero)
                kd_ratio = killer_doc['prey'][victim_id]['kills'] / max(1, reverse_kill_count)
                killer_doc['prey'][victim_id]['kd_ratio'] = round(kd_ratio, 2)
            
            # Update top_prey field for easy access
            prey_list = sorted(
                [(v['kills'], v['player_id'], v.get('kd_ratio', 0.0)) for k, v in killer_doc['prey'].items()],
                key=lambda x: (x[0], x[2]),  # Sort by kills, then K/D as tiebreaker
                reverse=True
            )
            
            if prey_list is not None and len(prey_list) > 0:
                top_prey_id = prey_list[0][1]
                # Store top prey information for easy access
                killer_doc['top_prey'] = {
                    'player_id': top_prey_id,
                    'player_name': killer_doc['prey'][top_prey_id]['player_name'],
                    'kills': killer_doc['prey'][top_prey_id]['kills'],
                    'kd_ratio': killer_doc['prey'][top_prey_id].get('kd_ratio', 0.0)
                }
            
            # Update the player document
            await db.players.update_one(
                {"server_id": server_id, "player_id": killer_id},
                {"$set": {
                    "prey": killer_doc['prey'],
                    "top_prey": killer_doc.get('top_prey')
                }}
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating prey data: {e}")
            return False
    
    @staticmethod
    async def _update_nemesis_data(db, server_id: str, victim_id: str, killer_id: str) -> bool:
        """Updates a player's "nemesis" data (player they're most frequently killed by)
        
        Args:
            db: Database connection
            server_id: Game server ID
            victim_id: Player who was killed
            killer_id: Player who did the killing
            
        Returns:
            True if updated is not None successfully, False otherwise
        """
        try:
            # Get current player data
            victim_doc = await db.players.find_one({
                "server_id": server_id, 
                "player_id": victim_id
            })
            
            if victim_doc is None:
                logger.warning(f"Cannot update nemesis: Victim {victim_id} not found in database")
                return False
            
            # Get killer data for name and K/D calculation
            killer_doc = await db.players.find_one({
                "server_id": server_id, 
                "player_id": killer_id
            })
            
            # Initialize nemesis data if it doesn\'t exist
            if 'nemesis' not in victim_doc:
                victim_doc['nemesis'] = {}
            
            # Initialize or update this killer in nemesis data
            if killer_id is not None not in victim_doc['nemesis']:
                victim_doc['nemesis'][killer_id] = {
                    'kills': 1,
                    'player_id': killer_id,
                    'player_name': killer_doc['name'] if killer_doc is not None else "Unknown Player",
                    'last_kill': datetime.utcnow()
                }
            else:
                victim_doc['nemesis'][killer_id]['kills'] += 1
                victim_doc['nemesis'][killer_id]['last_kill'] = datetime.utcnow()
                if killer_doc is not None:
                    victim_doc['nemesis'][killer_id]['player_name'] = killer_doc['name']
            
            # Calculate K/D ratio for this nemesis
            if killer_doc is not None:
                # Get the reverse kill count (how many times victim killed the killer)
                prey_data = victim_doc.get('prey', {}).get(killer_id, {})
                reverse_kill_count = prey_data.get('kills', 0) if prey_data is not None else 0
                
                # Calculate K/D (use 1 for zero deaths to avoid divide by zero)
                # Note: For nemesis this is reversed - it's how many times they killed the player
                kd_ratio = victim_doc['nemesis'][killer_id]['kills'] / max(1, reverse_kill_count)
                victim_doc['nemesis'][killer_id]['kd_ratio'] = round(kd_ratio, 2)
            
            # Update top_nemesis field for easy access
            nemesis_list = sorted(
                [(v['kills'], v['player_id'], v.get('kd_ratio', 0.0)) for k, v in victim_doc['nemesis'].items()],
                key=lambda x: (x[0], x[2]),  # Sort by kills, then K/D as tiebreaker
                reverse=True
            )
            
            if nemesis_list is not None and len(nemesis_list) > 0:
                top_nemesis_id = nemesis_list[0][1]
                # Store top nemesis information for easy access
                victim_doc['top_nemesis'] = {
                    'player_id': top_nemesis_id,
                    'player_name': victim_doc['nemesis'][top_nemesis_id]['player_name'],
                    'kills': victim_doc['nemesis'][top_nemesis_id]['kills'],
                    'kd_ratio': victim_doc['nemesis'][top_nemesis_id].get('kd_ratio', 0.0)
                }
            
            # Update the player document
            await db.players.update_one(
                {"server_id": server_id, "player_id": victim_id},
                {"$set": {
                    "nemesis": victim_doc['nemesis'],
                    "top_nemesis": victim_doc.get('top_nemesis')
                }}
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating nemesis data: {e}")
            return False
    
    @staticmethod
    async def get_player_rivalries(db, server_id: str, player_id: str) -> Dict[str, Any]:
        """Get rivalry data for a specific player
        
        Args:
            db: Database connection
            server_id: Game server ID
            player_id: Player ID
            
        Returns:
            Dictionary containing rivalry data (prey and nemesis)
        """
        player_doc = await db.players.find_one({
            "server_id": server_id,
            "player_id": player_id
        })
        
        if player_doc is None:
            return {
                "prey": None,
                "nemesis": None,
                "top_prey": None,
                "top_nemesis": None
            }
        
        return {
            "prey": player_doc.get("prey", {}),
            "nemesis": player_doc.get("nemesis", {}),
            "top_prey": player_doc.get("top_prey"),
            "top_nemesis": player_doc.get("top_nemesis")
        }
    
    @staticmethod
    async def get_top_rivalries(db, server_id: str, limit: int = 10) -> Dict[str, List[Dict[str, Any]]]:
        """Get top rivalry pairs across all players
        
        Args:
            db: Database connection
            server_id: Game server ID
            limit: Number of rivalries to return
            
        Returns:
            Dictionary containing top rivalries with "hunter" and "hunted" designations
        """
        # Find players with rivalries
        cursor = db.players.find({
            "server_id": server_id,
            "top_prey": {"$exists": True}
        })
        
        rivalries = []
        
        async for player in cursor:
            if "top_prey" in player and player["top_prey"]:
                prey_id = player["top_prey"]["player_id"]
                prey_kills = player["top_prey"]["kills"]
                prey_kd = player["top_prey"].get("kd_ratio", 0.0)
                
                # Only include significant rivalries (more than 3 kills)
                if prey_kills >= 3:
                    rivalries.append({
                        "hunter_id": player["player_id"],
                        "hunter_name": player["name"],
                        "hunted_id": prey_id,
                        "hunted_name": player["top_prey"]["player_name"],
                        "kill_count": prey_kills,
                        "kd_ratio": prey_kd,
                        "intensity": prey_kills * prey_kd  # Composite score of kills * K/D
                    })
        
        # Sort by intensity (kills * K/D)
        rivalries.sort(key=lambda x: x["intensity"], reverse=True)
        
        return {
            "top_rivalries": rivalries[:limit]
        }
    
    @staticmethod
    async def calculate_missing_rivalry_data(db, server_id: str) -> int:
        """Calculate rivalry data for existing players (backfill)
        
        Args:
            db: Database connection
            server_id: Game server ID
            
        Returns:
            Number of kills processed
        """
        logger.info(f"Starting rivalry data backfill for server {server_id}")
        
        # Get all kills for this server
        kill_cursor = db.kills.find({
            "server_id": server_id
        }).sort("timestamp", 1)  # Process in chronological order
        
        processed_count = 0
        
        # Process each kill
        async for kill in kill_cursor:
            await RivalryTracker.update_rivalry_on_kill(db, kill)
            processed_count += 1
            
            # Log progress
            if processed_count is not None % 1000 == 0:
                logger.info(f"Processed {processed_count} kills for rivalry calculation")
        
        logger.info(f"Completed rivalry data backfill: {processed_count} kills processed")
        return processed_count