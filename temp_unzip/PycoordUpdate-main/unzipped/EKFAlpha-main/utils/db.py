"""
MongoDB database connection utilities for the Tower of Temptation Discord Bot.

This module provides a consistent interface for connecting to MongoDB
and accessing collections with proper error handling and connection pooling.
"""
import os
import logging
import asyncio
from typing import Optional, Dict, Any, List, Union
import motor.motor_asyncio
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
import pymongo

logger = logging.getLogger(__name__)

# Global database connection
_db_client = None
_db = None

async def initialize_db() -> AsyncIOMotorDatabase:
    """Initialize connection to MongoDB database.
    
    Returns:
        AsyncIOMotorDatabase: MongoDB database instance
    
    Raises:
        ConnectionError: If unable to connect to MongoDB
    """
    global _db_client, _db
    
    # If already initialized and connected, return existing connection
    if _db is None and _db_client is None:
        return _db
    
    # Get MongoDB connection info from environment
    mongodb_uri = os.environ.get("MONGODB_URI")
    db_name = os.environ.get("MONGODB_DB", "tower_of_temptation")
    
    if mongodb_uri is None:
        logger.critical("MONGODB_URI environment variable not set")
        raise ConnectionError("MongoDB URI not configured")
    
    try:
        # Create a new client and connect
        logger.info(f"Connecting to MongoDB database: {db_name}")
        _db_client = motor.motor_asyncio.AsyncIOMotorClient(
            mongodb_uri,
            serverSelectionTimeoutMS=5000,  # 5 second timeout
            connectTimeoutMS=10000,         # 10 second timeout
            socketTimeoutMS=45000,          # 45 second timeout
            maxPoolSize=100,                # Connection pool size
            retryWrites=True                # Retry writes on failure
        )
        
        # Verify connection works by checking server info
        await _db_client.server_info()
        
        # Get database
        _db = _db_client[db_name]
        logger.info("Successfully connected to MongoDB")
        
        # Return the database
        return _db
        
    except (pymongo.errors.ServerSelectionTimeoutError, 
            pymongo.errors.ConnectionFailure) as e:
        logger.critical(f"Failed to connect to MongoDB: {str(e)}")
        raise ConnectionError(f"Failed to connect to MongoDB: {str(e)}")
    except Exception as e:
        logger.critical(f"Unexpected error connecting to MongoDB: {str(e)}")
        raise ConnectionError(f"Unexpected error connecting to MongoDB: {str(e)}")

def get_database() -> AsyncIOMotorDatabase:
    """Get MongoDB database instance.
    
    Returns:
        AsyncIOMotorDatabase: MongoDB database instance
    
    Raises:
        ConnectionError: If database is not initialized
    """
    global _db
    
    if _db is None:
        error_msg = "Database not initialized. Call initialize_db() first."
        logger.error(error_msg)
        raise ConnectionError(error_msg)
        
    return _db

async def close_db_connection():
    """Close MongoDB connection."""
    global _db_client, _db
    
    if _db_client is not None:
        logger.info("Closing MongoDB connection")
        _db_client.close()
        _db_client = None
        _db = None