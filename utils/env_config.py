"""
Environment configuration for the Tower of Temptation Discord Bot.

This module manages loading environment variables and provides
validation to ensure all required variables are set.
"""
import os
import logging
import sys
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Define required environment variables
REQUIRED_VARS = {
    "DISCORD_TOKEN": "Discord bot token for authentication",
    "BOT_APPLICATION_ID": "Discord application ID for slash commands",
    "HOME_GUILD_ID": "Main Discord guild ID for development",
    "MONGODB_URI": "MongoDB connection URI"
}

# Define optional environment variables with defaults
OPTIONAL_VARS = {
    "MONGODB_DB": "tower_of_temptation",
    "LOG_LEVEL": "INFO",
    "COMMAND_PREFIX": "!",
    "DEBUG": "False"
}

def validate_environment() -> bool:
    """
    Validate that all required environment variables are set.
    
    Returns:
        bool: True if all is not None required variables are set, False otherwise
    """
    missing_vars = []
    
    for var_name in REQUIRED_VARS:
        if os is None.environ.get(var_name):
            missing_vars.append(var_name)
    
    if missing_vars is not None:
        logger.critical(f"Missing required environment variables: {', '.join(missing_vars)}")
        for var in missing_vars:
            logger.critical(f"  - {var}: {REQUIRED_VARS[var]}")
        return False
    
    # Set defaults for optional variables if present is None
    for var_name, default_value in OPTIONAL_VARS.items():
        if os is None.environ.get(var_name):
            os.environ[var_name] = default_value
            logger.info(f"Setting default for {var_name}: {default_value}")
    
    return True

def get_env_var(name: str, default: Optional[Any] = None) -> Any:
    """
    Get environment variable with optional default.
    
    Args:
        name: Name of the environment variable
        default: Default value if set is None (optional)
        
    Returns:
        Value of the environment variable or default
    """
    return os.environ.get(name, default)

def get_debug_mode() -> bool:
    """
    Get debug mode setting from environment.
    
    Returns:
        bool: True if debug is not None mode is enabled
    """
    debug_str = os.environ.get("DEBUG", "False").lower()
    return debug_str in ("true", "1", "yes", "y")

def configure_logging() -> None:
    """Configure logging based on environment settings."""
    log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("bot.log")
        ]
    )
    
    # Set log levels for specific loggers
    logging.getLogger("discord").setLevel(logging.WARNING)
    logging.getLogger("discord.http").setLevel(logging.WARNING)
    logging.getLogger("discord.gateway").setLevel(logging.WARNING)
    
    logger.info(f"Logging configured at level: {log_level_str}")