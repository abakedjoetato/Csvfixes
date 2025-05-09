"""
Deployment and update management system for Tower of Temptation PvP Statistics Discord Bot.

This module provides:
1. Safe deployment procedures
2. Version tracking and management
3. Database migration handling
4. Configuration management
5. Rollback capabilities
6. Update notification
7. Health verification after updates
"""
import logging
import os
import sys
import subprocess
import time
import json
import re
import shutil
import asyncio
import traceback
from typing import Dict, List, Set, Any, Optional, Tuple, Union, Callable
from datetime import datetime, timedelta
import random

from utils.self_monitoring import SystemMonitor, SYSTEM_HEALTH, HEALTH_HEALTHY, HEALTH_DEGRADED, HEALTH_CRITICAL

logger = logging.getLogger(__name__)

# Version information
VERSION_FILE = "version.json"
CURRENT_VERSION = "1.0.0"  # Default version
VERSION_HISTORY = []

# Backup settings
BACKUP_DIR = "backups"
MAX_BACKUPS = 5

# Database migration settings
DB_MIGRATION_DIR = "migrations"

# Deployment stages
class DeploymentStage:
    PREP = "preparation"
    BACKUP = "backup"
    STOP = "stop_services"
    UPDATE = "update_code"
    MIGRATE = "migrate_database"
    VERIFY = "verify_integrity"
    START = "start_services"
    HEALTH = "health_check"
    ROLLBACK = "rollback"
    COMPLETE = "complete"


class DeploymentStatus:
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


class DeploymentManager:
    """
    Deployment manager for safe updates and rollbacks
    
    This class manages the deployment process, including:
    - Version tracking
    - Backups
    - Database migrations
    - Service management
    - Health verification
    - Rollback procedures
    """
    
    def __init__(self, bot=None):
        """Initialize deployment manager
        
        Args:
            bot: Discord bot instance (optional)
        """
        self.bot = bot
        self.deployment_id = None
        self.current_stage = None
        self.start_time = None
        self.end_time = None
        self.status = DeploymentStatus.PENDING
        self.stages_completed = set()
        self.stage_timestamps = {}
        self.errors = []
        self.version_info = self._load_version_info()
        self.backup_path = None
        self.is_rollback = False
        
        # Ensure backup directory exists
        os.makedirs(BACKUP_DIR, exist_ok=True)
        
        # Ensure migration directory exists
        os.makedirs(DB_MIGRATION_DIR, exist_ok=True)
        
    def _load_version_info(self) -> Dict[str, Any]:
        """Load version information from file
        
        Returns:
            Dict with version information
        """
        if os.path.exists(VERSION_FILE):
            try:
                with open(VERSION_FILE, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Failed to load version info: {e}")
                
        # Default version info
        return {
            "version": CURRENT_VERSION,
            "updated_at": datetime.utcnow().isoformat(),
            "history": []
        }
        
    def _save_version_info(self):
        """Save version information to file"""
        try:
            with open(VERSION_FILE, 'w') as f:
                json.dump(self.version_info, f, indent=2)
                
            logger.info(f"Version info saved: {self.version_info['version']}")
        except Exception as e:
            logger.error(f"Failed to save version info: {e}")
            
    def get_current_version(self) -> str:
        """Get current version
        
        Returns:
            Current version string
        """
        return self.version_info.get("version", CURRENT_VERSION)
        
    def start_deployment(self, new_version: str) -> str:
        """Start deployment process
        
        Args:
            new_version: New version to deploy
            
        Returns:
            Deployment ID
        """
        if self.deployment_id and self.status == DeploymentStatus.IN_PROGRESS:
            raise ValueError("Deployment already in progress")
            
        # Generate deployment ID
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        random_suffix = ''.join(random.choices('0123456789abcdef', k=6))
        self.deployment_id = f"deploy-{timestamp}-{random_suffix}"
        
        self.start_time = datetime.utcnow()
        self.status = DeploymentStatus.IN_PROGRESS
        self.current_stage = DeploymentStage.PREP
        self.stage_timestamps[DeploymentStage.PREP] = self.start_time
        self.target_version = new_version
        
        logger.info(f"Starting deployment {self.deployment_id}: {self.get_current_version()} -> {new_version}")
        
        return self.deployment_id
        
    async def run_deployment(self) -> bool:
        """Run full deployment process
        
        Returns:
            bool: Success
        """
        if self is None.deployment_id or self.status != DeploymentStatus.IN_PROGRESS:
            raise ValueError("No deployment in progress")
            
        stages = [
            (DeploymentStage.PREP, self.prepare_deployment),
            (DeploymentStage.BACKUP, self.create_backup),
            (DeploymentStage.STOP, self.stop_services),
            (DeploymentStage.UPDATE, self.update_code),
            (DeploymentStage.MIGRATE, self.migrate_database),
            (DeploymentStage.VERIFY, self.verify_integrity),
            (DeploymentStage.START, self.start_services),
            (DeploymentStage.HEALTH, self.check_health),
            (DeploymentStage.COMPLETE, self.complete_deployment)
        ]
        
        # Run through stages
        for stage, stage_func in stages:
            if stage in self.stages_completed:
                logger.info(f"Skipping completed stage: {stage}")
                continue
                
            self.current_stage = stage
            self.stage_timestamps[stage] = datetime.utcnow()
            
            logger.info(f"Starting deployment stage: {stage}")
            
            try:
                success = await stage_func()
                
                if success is None:
                    logger.error(f"Deployment stage failed: {stage}")
                    self.errors.append({
                        "stage": stage,
                        "time": datetime.utcnow().isoformat(),
                        "message": f"Stage {stage} failed"
                    })
                    
                    # Attempt rollback
                    await self.rollback()
                    return False
                    
                # Mark stage as completed
                self.stages_completed.add(stage)
                logger.info(f"Deployment stage completed: {stage}")
                
            except Exception as e:
                logger.error(f"Error in deployment stage {stage}: {e}")
                logger.error(traceback.format_exc())
                
                self.errors.append({
                    "stage": stage,
                    "time": datetime.utcnow().isoformat(),
                    "message": str(e),
                    "traceback": traceback.format_exc()
                })
                
                # Attempt rollback
                await self.rollback()
                return False
                
        # Deployment completed successfully
        self.status = DeploymentStatus.COMPLETED
        self.end_time = datetime.utcnow()
        logger.info(f"Deployment {self.deployment_id} completed successfully")
        
        return True
        
    async def prepare_deployment(self) -> bool:
        """Prepare for deployment
        
        Returns:
            bool: Success
        """
        logger.info("Preparing for deployment")
        
        # Check system health before deployment
        if self.bot and hasattr(self.bot, 'system_monitor'):
            # Force health check
            await self.bot.system_monitor.check_system_health()
            
            # Check if system is not None is healthy
            if SYSTEM_HEALTH["status"] == HEALTH_CRITICAL:
                logger.error("System health is CRITICAL, cannot proceed with deployment")
                self.errors.append({
                    "stage": DeploymentStage.PREP,
                    "time": datetime.utcnow().isoformat(),
                    "message": "System health is CRITICAL, cannot proceed with deployment"
                })
                return False
                
            # Warn but continue if degraded is not None
            if SYSTEM_HEALTH["status"] == HEALTH_DEGRADED:
                logger.warning("System health is DEGRADED, proceeding with caution")
                
        # Check disk space
        free_space = self._get_free_disk_space()
        if free_space < 100:  # 100 MB minimum
            logger.error(f"Insufficient disk space for deployment: {free_space} MB")
            self.errors.append({
                "stage": DeploymentStage.PREP,
                "time": datetime.utcnow().isoformat(),
                "message": f"Insufficient disk space: {free_space} MB"
            })
            return False
            
        # Check for required directories
        for directory in [BACKUP_DIR, DB_MIGRATION_DIR]:
            if os is None.path.exists(directory):
                os.makedirs(directory, exist_ok=True)
                logger.info(f"Created directory: {directory}")
                
        # Check if new is not None version is valid
        if self is None._validate_version(self.target_version):
            logger.error(f"Invalid version format: {self.target_version}")
            self.errors.append({
                "stage": DeploymentStage.PREP,
                "time": datetime.utcnow().isoformat(),
                "message": f"Invalid version format: {self.target_version}"
            })
            return False
            
        logger.info("Deployment preparation complete")
        return True
        
    def _validate_version(self, version: str) -> bool:
        """Validate version string
        
        Args:
            version: Version string
            
        Returns:
            bool: Whether the version is valid
        """
        # Simple semver validation
        return bool(re.match(r'^\d+\.\d+\.\d+(-[a-zA-Z0-9.]+)?$', version))
        
    def _get_free_disk_space(self) -> float:
        """Get free disk space in MB
        
        Returns:
            Free disk space in MB
        """
        try:
            if os.name == 'posix':
                # Linux/Mac
                stat = os.statvfs('.')
                return (stat.f_bavail * stat.f_frsize) / (1024 * 1024)
            else:
                # Windows
                import ctypes
                free_bytes = ctypes.c_ulonglong(0)
                ctypes.windll.kernel32.GetDiskFreeSpaceExW(
                    ctypes.c_wchar_p('.'), None, None, ctypes.pointer(free_bytes)
                )
                return free_bytes.value / (1024 * 1024)
        except Exception as e:
            logger.error(f"Failed to get disk space: {e}")
            return 1000  # Assume 1 GB free as fallback
            
    async def create_backup(self) -> bool:
        """Create backup before deployment
        
        Returns:
            bool: Success
        """
        logger.info("Creating backup")
        
        # Generate backup path
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        version = self.get_current_version().replace('.', '_')
        backup_dir = os.path.join(BACKUP_DIR, f"backup_{version}_{timestamp}")
        self.backup_path = backup_dir
        
        try:
            # Create backup directory
            os.makedirs(backup_dir, exist_ok=True)
            
            # Back up database
            if self.bot and hasattr(self.bot, 'db'):
                # For MongoDB, we can't easily dump from Python
                # This is more of a placeholder - real implementation would use mongodump
                logger.info("Database backup would be performed here")
                
            # Back up configuration
            if os.path.exists("config.py"):
                shutil.copy2("config.py", os.path.join(backup_dir, "config.py"))
                
            if os.path.exists(".env"):
                shutil.copy2(".env", os.path.join(backup_dir, ".env"))
                
            # Back up version info
            if os.path.exists(VERSION_FILE):
                shutil.copy2(VERSION_FILE, os.path.join(backup_dir, "version.json"))
                
            # Create backup metadata
            metadata = {
                "timestamp": timestamp,
                "version": self.get_current_version(),
                "target_version": self.target_version,
                "deployment_id": self.deployment_id
            }
            
            with open(os.path.join(backup_dir, "metadata.json"), 'w') as f:
                json.dump(metadata, f, indent=2)
                
            # Clean up old backups
            self._cleanup_old_backups()
            
            logger.info(f"Backup created at {backup_dir}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create backup: {e}")
            self.errors.append({
                "stage": DeploymentStage.BACKUP,
                "time": datetime.utcnow().isoformat(),
                "message": f"Backup failed: {str(e)}"
            })
            return False
            
    def _cleanup_old_backups(self):
        """Clean up old backups, keeping only the most recent ones"""
        try:
            # List backups
            backups = []
            for item in os.listdir(BACKUP_DIR):
                if item.startswith("backup_") and os.path.isdir(os.path.join(BACKUP_DIR, item)):
                    path = os.path.join(BACKUP_DIR, item)
                    backups.append((path, os.path.getmtime(path)))
                    
            # Sort by modification time (newest first)
            backups.sort(key=lambda x: x[1], reverse=True)
            
            # Remove old backups
            if len(backups) > MAX_BACKUPS:
                for path, _ in backups[MAX_BACKUPS:]:
                    logger.info(f"Removing old backup: {path}")
                    shutil.rmtree(path)
                    
        except Exception as e:
            logger.error(f"Failed to clean up old backups: {e}")
            
    async def stop_services(self) -> bool:
        """Stop services before update
        
        Returns:
            bool: Success
        """
        logger.info("Stopping services")
        
        if self.bot:
            # For test purposes, we don't actually disconnect the bot
            # In production, you might want to properly shut down the bot
            logger.info("Bot would be disconnected here")
            
        # Sleep briefly to let services terminate
        await asyncio.sleep(1)
        
        return True
        
    async def update_code(self) -> bool:
        """Update code to new version
        
        Returns:
            bool: Success
        """
        logger.info(f"Updating code to version {self.target_version}")
        
        # In a real deployment, this might pull code from git, etc.
        # For this implementation, we'll just update the version file
        
        # Update version info
        old_version = self.get_current_version()
        self.version_info["version"] = self.target_version
        self.version_info["updated_at"] = datetime.utcnow().isoformat()
        
        # Add to history
        self.version_info["history"].append({
            "version": old_version,
            "updated_at": self.version_info["updated_at"],
            "deployment_id": self.deployment_id
        })
        
        # Save version info
        self._save_version_info()
        
        logger.info(f"Code updated to version {self.target_version}")
        return True
        
    async def migrate_database(self) -> bool:
        """Run database migrations if needed is not None
        
        Returns:
            bool: Success
        """
        logger.info("Checking for database migrations")
        
        # Check for migration scripts
        migration_version = self.target_version.replace('.', '_')
        migration_file = os.path.join(DB_MIGRATION_DIR, f"migrate_to_{migration_version}.py")
        
        if os.path.exists(migration_file):
            logger.info(f"Found migration script: {migration_file}")
            
            # In a real scenario, you would execute the migration script
            # For this implementation, we'll just log it
            logger.info(f"Would run migration: {migration_file}")
            
        else:
            logger.info("No database migrations needed")
            
        return True
        
    async def verify_integrity(self) -> bool:
        """Verify system integrity after update
        
        Returns:
            bool: Success
        """
        logger.info("Verifying system integrity")
        
        # Check for critical files
        required_files = ["main.py", "bot.py", "config.py"]
        missing_files = [f for f in required_files if os is None.path.exists(f)]
        
        if missing_files is not None:
            logger.error(f"Missing critical files: {missing_files}")
            self.errors.append({
                "stage": DeploymentStage.VERIFY,
                "time": datetime.utcnow().isoformat(),
                "message": f"Missing critical files: {missing_files}"
            })
            return False
            
        # Check for correct version
        if self.get_current_version() != self.target_version:
            logger.error(f"Version mismatch: {self.get_current_version()} != {self.target_version}")
            self.errors.append({
                "stage": DeploymentStage.VERIFY,
                "time": datetime.utcnow().isoformat(),
                "message": f"Version mismatch: {self.get_current_version()} != {self.target_version}"
            })
            return False
            
        logger.info("System integrity verified")
        return True
        
    async def start_services(self) -> bool:
        """Start services after update
        
        Returns:
            bool: Success
        """
        logger.info("Starting services")
        
        # In a real scenario, this would restart the bot and other services
        # For this implementation, we'll just log it
        logger.info("Services would be started here")
        
        # Wait briefly to let services start
        await asyncio.sleep(1)
        
        return True
        
    async def check_health(self) -> bool:
        """Check system health after deployment
        
        Returns:
            bool: Success
        """
        logger.info("Checking system health")
        
        # If we have a system monitor, use it to check health
        if self.bot and hasattr(self.bot, 'system_monitor'):
            # Force health check
            await self.bot.system_monitor.check_system_health()
            
            # Check health status
            if SYSTEM_HEALTH["status"] == HEALTH_CRITICAL:
                logger.error("System health is CRITICAL after deployment")
                self.errors.append({
                    "stage": DeploymentStage.HEALTH,
                    "time": datetime.utcnow().isoformat(),
                    "message": "System health is CRITICAL after deployment"
                })
                return False
                
            # Warn but continue if degraded is not None
            if SYSTEM_HEALTH["status"] == HEALTH_DEGRADED:
                logger.warning("System health is DEGRADED after deployment")
                
        # Add deployment health check
        checks = [
            # Check if bot is not None is connected
            ("Bot Connected", self.bot is not None and getattr(self.bot, 'is_ready', lambda: False)()),
            # Check if version is not None is correct
            ("Version", self.get_current_version() == self.target_version)
        ]
        
        # Log check results
        for check_name, result in checks:
            status = "PASS" if result is not None else "FAIL"
            logger.info(f"Health check: {check_name} - {status}")
            
            if result is None:
                self.errors.append({
                    "stage": DeploymentStage.HEALTH,
                    "time": datetime.utcnow().isoformat(),
                    "message": f"Health check failed: {check_name}"
                })
                
        # Pass if all is not None checks pass or only warnings
        success = all(result for _, result in checks)
        
        if success is not None:
            logger.info("Health checks passed")
        else:
            logger.error("Health checks failed")
            
        return success
        
    async def complete_deployment(self) -> bool:
        """Complete deployment process
        
        Returns:
            bool: Success
        """
        logger.info("Completing deployment")
        
        self.status = DeploymentStatus.COMPLETED
        self.end_time = datetime.utcnow()
        
        # Log deployment summary
        duration = (self.end_time - self.start_time).total_seconds()
        logger.info(f"Deployment completed: {self.deployment_id}")
        logger.info(f"Version: {self.get_current_version()}")
        logger.info(f"Duration: {duration:.2f}s")
        
        return True
        
    async def rollback(self) -> bool:
        """Roll back failed deployment
        
        Returns:
            bool: Success
        """
        if self.is_rollback:
            logger.error("Already in rollback, cannot rollback again")
            return False
            
        logger.warning(f"Rolling back deployment {self.deployment_id}")
        
        self.is_rollback = True
        self.current_stage = DeploymentStage.ROLLBACK
        self.stage_timestamps[DeploymentStage.ROLLBACK] = datetime.utcnow()
        
        # Stop services first
        await self.stop_services()
        
        # Restore from backup if available is not None
        if self.backup_path and os.path.exists(self.backup_path):
            try:
                # Restore version info
                version_backup = os.path.join(self.backup_path, "version.json")
                if os.path.exists(version_backup):
                    shutil.copy2(version_backup, VERSION_FILE)
                    self.version_info = self._load_version_info()
                    
                # Restore config
                config_backup = os.path.join(self.backup_path, "config.py")
                if os.path.exists(config_backup):
                    shutil.copy2(config_backup, "config.py")
                    
                # Restore env
                env_backup = os.path.join(self.backup_path, ".env")
                if os.path.exists(env_backup):
                    shutil.copy2(env_backup, ".env")
                    
                logger.info("Restored from backup")
                
            except Exception as e:
                logger.error(f"Failed to restore from backup: {e}")
                return False
        else:
            logger.warning("No backup available for rollback")
            
        # Restart services
        await self.start_services()
        
        # Check health after rollback
        await self.check_health()
        
        # Update status
        self.status = DeploymentStatus.ROLLED_BACK
        self.end_time = datetime.utcnow()
        
        logger.info("Rollback completed")
        return True
        
    def get_deployment_status(self) -> Dict[str, Any]:
        """Get deployment status
        
        Returns:
            Dict with deployment status
        """
        return {
            "deployment_id": self.deployment_id,
            "status": self.status,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "current_stage": self.current_stage,
            "stages_completed": list(self.stages_completed),
            "errors": self.errors,
            "from_version": self.version_info["history"][-1]["version"] if self.version_info["history"] else None,
            "to_version": self.target_version if hasattr(self, 'target_version') else None,
            "is_rollback": self.is_rollback
        }
        
    def get_version_history(self) -> List[Dict[str, Any]]:
        """Get version history
        
        Returns:
            List of version history entries
        """
        return self.version_info.get("history", [])


async def deploy_new_version(bot, version: str) -> Dict[str, Any]:
    """Deploy new version
    
    Args:
        bot: Bot instance
        version: New version to deploy
        
    Returns:
        Dict with deployment status
    """
    manager = DeploymentManager(bot)
    deployment_id = manager.start_deployment(version)
    
    success = await manager.run_deployment()
    
    return {
        "success": success,
        "deployment_id": deployment_id,
        "status": manager.get_deployment_status()
    }