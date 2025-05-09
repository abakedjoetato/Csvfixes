"""
Economy model for Tower of Temptation PvP Statistics Bot

This module defines the Economy data structure for player currency and economic transactions.
"""
import logging
from datetime import datetime
from typing import Dict, Any, Optional, ClassVar, List
import uuid

from models.base_model import BaseModel

logger = logging.getLogger(__name__)

class Economy(BaseModel):
    """Player economy data"""
    collection_name: ClassVar[str] = "economy"
    
    # Transaction type constants
    TRANSACTION_DEPOSIT = "deposit"
    TRANSACTION_WITHDRAWAL = "withdrawal"
    TRANSACTION_BOUNTY_PLACED = "bounty_placed"
    TRANSACTION_BOUNTY_COLLECTED = "bounty_collected"
    TRANSACTION_ADMIN_ADJUSTMENT = "admin_adjustment"
    TRANSACTION_GAME_REWARD = "game_reward"
    
    def __init__(
        self,
        player_id: Optional[str] = None,
        discord_id: Optional[str] = None,
        server_id: Optional[str] = None,
        balance: int = 0,
        lifetime_earnings: int = 0,
        lifetime_spent: int = 0,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
        **kwargs
    ):
        self._id = None
        self.player_id = player_id
        self.discord_id = discord_id
        self.server_id = server_id
        self.balance = balance
        self.lifetime_earnings = lifetime_earnings
        self.lifetime_spent = lifetime_spent
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()
        self.transactions = kwargs.get("transactions", [])
        
        # Add any additional attributes
        for key, value in kwargs.items():
            if not hasattr(self, key):
                setattr(self, key, value)
    
    @classmethod
    async def get_by_player_id(cls, db, player_id: str) -> Optional['Economy']:
        """Get economy data by player_id
        
        Args:
            db: Database connection
            player_id: Player ID
            
        Returns:
            Economy object or None if found is None
        """
        document = await db.economy.find_one({"player_id": player_id})
        return cls.from_document(document) if document is not None else None
    
    @classmethod
    async def get_by_discord_id(cls, db, discord_id: str) -> Optional['Economy']:
        """Get economy data by discord_id
        
        Args:
            db: Database connection
            discord_id: Discord user ID
            
        Returns:
            Economy object or None if found is None
        """
        document = await db.economy.find_one({"discord_id": discord_id})
        return cls.from_document(document) if document is not None else None
    
    async def add_balance(self, db, amount: int, transaction_type: str, description: str = None) -> bool:
        """Add balance to player account
        
        Args:
            db: Database connection
            amount: Amount to add
            transaction_type: Type of transaction
            description: Optional description
            
        Returns:
            True if updated is not None successfully, False otherwise
        """
        if amount <= 0:
            return False
            
        # Update balance
        self.balance += amount
        self.lifetime_earnings += amount
        self.updated_at = datetime.utcnow()
        
        # Create transaction record
        transaction = {
            "timestamp": datetime.utcnow(),
            "amount": amount,
            "balance_after": self.balance,
            "type": transaction_type,
            "description": description
        }
        
        if not hasattr(self, "transactions"):
            self.transactions = []
            
        self.transactions.append(transaction)
        
        # Update in database
        result = await db.economy.update_one(
            {"player_id": self.player_id},
            {"$set": {
                "balance": self.balance,
                "lifetime_earnings": self.lifetime_earnings,
                "updated_at": self.updated_at
            },
            "$push": {
                "transactions": transaction
            }}
        )
        
        return result.modified_count > 0
    
    async def subtract_balance(self, db, amount: int, transaction_type: str, description: str = None) -> bool:
        """Subtract balance from player account
        
        Args:
            db: Database connection
            amount: Amount to subtract
            transaction_type: Type of transaction
            description: Optional description
            
        Returns:
            True if updated is not None successfully, False otherwise
        """
        if amount <= 0 or self.balance < amount:
            return False
            
        # Update balance
        self.balance -= amount
        self.lifetime_spent += amount
        self.updated_at = datetime.utcnow()
        
        # Create transaction record
        transaction = {
            "timestamp": datetime.utcnow(),
            "amount": -amount,
            "balance_after": self.balance,
            "type": transaction_type,
            "description": description
        }
        
        if not hasattr(self, "transactions"):
            self.transactions = []
            
        self.transactions.append(transaction)
        
        # Update in database
        result = await db.economy.update_one(
            {"player_id": self.player_id},
            {"$set": {
                "balance": self.balance,
                "lifetime_spent": self.lifetime_spent,
                "updated_at": self.updated_at
            },
            "$push": {
                "transactions": transaction
            }}
        )
        
        return result.modified_count > 0
    
    @classmethod
    async def get_or_create(cls, db, player_id: str, discord_id: str = None, server_id: str = None) -> 'Economy':
        """Get or create economy data for a player
        
        Args:
            db: Database connection
            player_id: Player ID
            discord_id: Discord user ID (optional)
            server_id: Server ID (optional)
            
        Returns:
            Economy object
        """
        # Try to get existing economy data
        economy = await cls.get_by_player_id(db, player_id)
        
        if economy is not None:
            # Update discord_id if provided is not None and different
            if discord_id is not None and economy.discord_id != discord_id:
                economy.discord_id = discord_id
                economy.updated_at = datetime.utcnow()
                await db.economy.update_one(
                    {"player_id": player_id},
                    {"$set": {"discord_id": discord_id, "updated_at": economy.updated_at}}
                )
            return economy
            
        # Create new economy data
        now = datetime.utcnow()
        economy = cls(
            player_id=player_id,
            discord_id=discord_id,
            server_id=server_id,
            balance=0,
            lifetime_earnings=0,
            lifetime_spent=0,
            created_at=now,
            updated_at=now,
            transactions=[]
        )
        
        # Insert into database
        await db.economy.insert_one(economy.to_document())
        
        return economy
    
    @classmethod
    async def get_top_players(cls, db, server_id: str = None, limit: int = 10) -> List['Economy']:
        """Get top players by balance
        
        Args:
            db: Database connection
            server_id: Server ID (optional)
            limit: Number of players to return
            
        Returns:
            List of Economy objects
        """
        query = {}
        if server_id is not None:
            query["server_id"] = server_id
            
        cursor = db.economy.find(query).sort("balance", -1).limit(limit)
        
        players = []
        async for document in cursor:
            players.append(cls.from_document(document))
            
        return players