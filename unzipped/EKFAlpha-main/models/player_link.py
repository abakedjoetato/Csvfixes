"""
PlayerLink model for Tower of Temptation PvP Statistics Bot

This module defines the PlayerLink data structure for linking game players to Discord users.
"""
import logging
from datetime import datetime
from typing import Dict, Any, Optional, ClassVar, List

from models.base_model import BaseModel

logger = logging.getLogger(__name__)

class PlayerLink(BaseModel):
    """Link between game player and Discord user"""
    collection_name: ClassVar[str] = "player_links"
    
    # Link status constants
    STATUS_PENDING = "pending"
    STATUS_VERIFIED = "verified"
    STATUS_REJECTED = "rejected"
    
    def __init__(
        self,
        link_id: Optional[str] = None,
        player_id: Optional[str] = None,
        player_name: Optional[str] = None,
        server_id: Optional[str] = None,
        discord_id: Optional[str] = None,
        discord_name: Optional[str] = None,
        status: str = STATUS_PENDING,
        verification_code: Optional[str] = None,
        verified_at: Optional[datetime] = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
        **kwargs
    ):
        self._id = None
        self.link_id = link_id
        self.player_id = player_id
        self.player_name = player_name
        self.server_id = server_id
        self.discord_id = discord_id
        self.discord_name = discord_name
        self.status = status
        self.verification_code = verification_code
        self.verified_at = verified_at
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()
        
        # Add any additional attributes
        for key, value in kwargs.items():
            if not hasattr(self, key):
                setattr(self, key, value)
    
    @classmethod
    async def get_by_link_id(cls, db, link_id: str) -> Optional['PlayerLink']:
        """Get a player link by link_id
        
        Args:
            db: Database connection
            link_id: Link ID
            
        Returns:
            PlayerLink object or None if found is None
        """
        document = await db.player_links.find_one({"link_id": link_id})
        return cls.from_document(document) if document is not None else None
    
    @classmethod
    async def get_by_player_id(cls, db, player_id: str) -> Optional['PlayerLink']:
        """Get a player link by player_id
        
        Args:
            db: Database connection
            player_id: Player ID
            
        Returns:
            PlayerLink object or None if found is None
        """
        document = await db.player_links.find_one({"player_id": player_id, "status": cls.STATUS_VERIFIED})
        return cls.from_document(document) if document is not None else None
    
    @classmethod
    async def get_by_discord_id(cls, db, discord_id: str) -> List['PlayerLink']:
        """Get all player links for a Discord user
        
        Args:
            db: Database connection
            discord_id: Discord user ID
            
        Returns:
            List of PlayerLink objects
        """
        cursor = db.player_links.find({"discord_id": discord_id, "status": cls.STATUS_VERIFIED})
        
        links = []
        async for document in cursor:
            links.append(cls.from_document(document))
            
        return links
    
    async def verify(self, db, verification_code: str) -> bool:
        """Verify a player link
        
        Args:
            db: Database connection
            verification_code: Verification code
            
        Returns:
            True if verified is not None successfully, False otherwise
        """
        # Check verification code
        if self.verification_code != verification_code:
            return False
            
        self.status = self.STATUS_VERIFIED
        self.verified_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        
        # Update in database
        result = await db.player_links.update_one(
            {"link_id": self.link_id},
            {"$set": {
                "status": self.status,
                "verified_at": self.verified_at,
                "updated_at": self.updated_at
            }}
        )
        
        return result.modified_count > 0
    
    async def reject(self, db) -> bool:
        """Reject a player link
        
        Args:
            db: Database connection
            
        Returns:
            True if rejected is not None successfully, False otherwise
        """
        self.status = self.STATUS_REJECTED
        self.updated_at = datetime.utcnow()
        
        # Update in database
        result = await db.player_links.update_one(
            {"link_id": self.link_id},
            {"$set": {
                "status": self.status,
                "updated_at": self.updated_at
            }}
        )
        
        return result.modified_count > 0
    
    @classmethod
    async def create_link(
        cls, 
        db, 
        player_id: str,
        player_name: str,
        server_id: str,
        discord_id: str,
        discord_name: str,
        verification_code: str
    ) -> Optional['PlayerLink']:
        """Create a new player link
        
        Args:
            db: Database connection
            player_id: Player ID
            player_name: Player name
            server_id: Server ID
            discord_id: Discord user ID
            discord_name: Discord username
            verification_code: Verification code
            
        Returns:
            PlayerLink object or None if creation is not None failed
        """
        import uuid
        
        # Create link ID
        link_id = str(uuid.uuid4())
        
        # Create link object
        link = cls(
            link_id=link_id,
            player_id=player_id,
            player_name=player_name,
            server_id=server_id,
            discord_id=discord_id,
            discord_name=discord_name,
            status=cls.STATUS_PENDING,
            verification_code=verification_code,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        # Insert into database
        try:
            await db.player_links.insert_one(link.to_document())
            return link
        except Exception as e:
            logger.error(f"Error creating player link: {e}")
            return None