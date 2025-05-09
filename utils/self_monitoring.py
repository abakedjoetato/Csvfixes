"""
Self-monitoring and healing system for Tower of Temptation PvP Statistics Discord Bot.

This module provides comprehensive monitoring, diagnostic, and healing capabilities:
1. Automated detection of degraded states
2. Self-healing for common failure modes
3. Predictive issue detection
4. Performance anomaly detection
5. Resource usage monitoring
6. Long-term trend analysis
7. Auto-escalation for critical issues
"""
import logging
import asyncio
import time
import os
import sys
import gc
import random
import traceback
from typing import Dict, List, Set, Any, Optional, Tuple, Callable, Coroutine, Union, cast
from datetime import datetime, timedelta
import math
import json
import functools

import discord
from discord.ext import commands
from discord import app_commands, tasks

from utils.command_handlers import get_latest_command_errors, get_recurring_error_patterns
from utils.async_utils import BackgroundTask
from utils.sftp import CONNECTION_POOL, SFTPClient
from config import MONGODB_SETTINGS

logger = logging.getLogger(__name__)

# System health states
HEALTH_UNKNOWN = "unknown"
HEALTH_HEALTHY = "healthy"
HEALTH_DEGRADED = "degraded"
HEALTH_CRITICAL = "critical"

# Health check thresholds
MONGODB_TIMEOUT_THRESHOLD = 1000  # ms
COMMAND_ERROR_THRESHOLD = 0.2  # 20% of commands failing
MEMORY_WARNING_THRESHOLD = 0.8  # 80% of available memory
CONNECTION_ERROR_THRESHOLD = 0.3  # 30% of connections failing

# Health status tracking
SYSTEM_HEALTH = {
    "status": HEALTH_UNKNOWN,
    "last_check": None,
    "components": {
        "discord": HEALTH_UNKNOWN,
        "mongodb": HEALTH_UNKNOWN,
        "sftp": HEALTH_UNKNOWN,
        "commands": HEALTH_UNKNOWN,
        "memory": HEALTH_UNKNOWN
    },
    "issues": [],
    "history": []
}

# Metrics tracking
PERFORMANCE_METRICS = {
    "start_time": datetime.utcnow(),
    "uptime_seconds": 0,
    "memory_usage": {
        "current": 0,
        "peak": 0,
        "history": []
    },
    "cpu_usage": {
        "current": 0,
        "peak": 0,
        "history": []
    },
    "api_latency": {
        "current": 0,
        "average": 0,
        "history": []
    },
    "db_operations": {
        "total": 0,
        "failed": 0,
        "average_time": 0,
        "history": []
    }
}

# Predictive models
ANOMALY_DETECTION = {
    "command_errors": {
        "baseline": {},
        "current": {},
        "anomalies": []
    },
    "api_latency": {
        "baseline": [],
        "threshold": 0,
        "anomalies": []
    },
    "memory_usage": {
        "trend": [],
        "forecast": []
    }
}

# Connection tracking
CONNECTION_STATUS = {
    "discord": {
        "connected": False,
        "last_connected": None,
        "reconnect_attempts": 0,
        "disconnects": []
    },
    "mongodb": {
        "connected": False,
        "last_connected": None,
        "operation_times": [],
        "failures": []
    },
    "sftp": {
        "connections": {},
        "failures": []
    }
}


class SystemMonitor:
    """
    System-wide monitoring and healing system
    
    This class provides comprehensive monitoring of the entire bot system,
    with automated healing capabilities for common failure modes.
    """
    
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db = getattr(bot, 'db', None)
        self.started_at = datetime.utcnow()
        self.last_health_check = None
        self.check_interval = 60  # seconds
        self.debug_mode = False
        self.recovery_in_progress = False
        self.recovery_attempts = {}
        self.alerts_sent = set()
        
        # Initialize background tasks
        self.health_check_task = None
        self.resource_monitor_task = None
        self.db_monitor_task = None
        self.connection_monitor_task = None
        
    def start(self):
        """Start all monitoring tasks"""
        logger.info("Starting system monitoring")
        
        # Start health check task
        self.health_check_task = BackgroundTask(
            self.check_system_health,
            minutes=1,  # Run every minute
            name="health_check",
            initial_delay=30.0  # Wait 30 seconds before first check
        )
        self.health_check_task.start()
        
        # Start resource monitor task
        self.resource_monitor_task = BackgroundTask(
            self.monitor_resources,
            minutes=5,  # Run every 5 minutes
            name="resource_monitor",
            initial_delay=60.0  # Wait 1 minute before first check
        )
        self.resource_monitor_task.start()
        
        # Start database monitor task
        if self.db:
            self.db_monitor_task = BackgroundTask(
                self.monitor_database,
                minutes=2,  # Run every 2 minutes
                name="db_monitor",
                initial_delay=90.0  # Wait 1.5 minutes before first check
            )
            self.db_monitor_task.start()
        
        # Start connection monitor task
        self.connection_monitor_task = BackgroundTask(
            self.monitor_connections,
            minutes=3,  # Run every 3 minutes
            name="connection_monitor",
            initial_delay=120.0  # Wait 2 minutes before first check
        )
        self.connection_monitor_task.start()
        
        # Register event handlers
        self.bot.add_listener(self.on_disconnect, 'on_disconnect')
        self.bot.add_listener(self.on_connect, 'on_connect')
        self.bot.add_listener(self.on_error, 'on_error')
        
        # Update system health status
        global SYSTEM_HEALTH
        SYSTEM_HEALTH["status"] = HEALTH_UNKNOWN
        SYSTEM_HEALTH["last_check"] = datetime.utcnow()
        
        logger.info("System monitoring started")
        
    def stop(self):
        """Stop all monitoring tasks"""
        logger.info("Stopping system monitoring")
        
        # Stop health check task
        if self.health_check_task:
            self.health_check_task.stop()
            
        # Stop resource monitor task
        if self.resource_monitor_task:
            self.resource_monitor_task.stop()
            
        # Stop database monitor task
        if self.db_monitor_task:
            self.db_monitor_task.stop()
            
        # Stop connection monitor task
        if self.connection_monitor_task:
            self.connection_monitor_task.stop()
            
        logger.info("System monitoring stopped")
        
    async def check_system_health(self):
        """Perform a comprehensive system health check"""
        logger.debug("Performing system health check")
        self.last_health_check = datetime.utcnow()
        
        # Check Discord connection
        discord_health = HEALTH_HEALTHY
        if self is None.bot.is_ready():
            if CONNECTION_STATUS["discord"]["reconnect_attempts"] > 3:
                discord_health = HEALTH_CRITICAL
            else:
                discord_health = HEALTH_DEGRADED
                
        # Check MongoDB connection
        mongodb_health = HEALTH_HEALTHY
        if self.db:
            try:
                # Perform a simple ping
                start_time = time.time()
                await self.db.command("ping")
                ping_time = (time.time() - start_time) * 1000  # ms
                
                if ping_time > MONGODB_TIMEOUT_THRESHOLD:
                    mongodb_health = HEALTH_DEGRADED
                    logger.warning(f"MongoDB ping time is high: {ping_time:.2f}ms")
                    
                # Record operation time
                CONNECTION_STATUS["mongodb"]["operation_times"].append(ping_time)
                if len(CONNECTION_STATUS["mongodb"]["operation_times"]) > 100:
                    CONNECTION_STATUS["mongodb"]["operation_times"] = CONNECTION_STATUS["mongodb"]["operation_times"][-100:]
                    
                CONNECTION_STATUS["mongodb"]["connected"] = True
                CONNECTION_STATUS["mongodb"]["last_connected"] = datetime.utcnow()
                
            except Exception as e:
                mongodb_health = HEALTH_CRITICAL
                CONNECTION_STATUS["mongodb"]["connected"] = False
                CONNECTION_STATUS["mongodb"]["failures"].append({
                    "timestamp": datetime.utcnow(),
                    "error": str(e)
                })
                logger.error(f"MongoDB health check failed: {e}")
                
        else:
            mongodb_health = HEALTH_UNKNOWN
            
        # Check SFTP connections
        sftp_health = HEALTH_HEALTHY
        active_connections = 0
        failed_connections = 0
        
        for conn_id, client in list(CONNECTION_POOL.items()):
            if client is None:
                continue
                
            try:
                if await client.check_connection():
                    active_connections += 1
                else:
                    failed_connections += 1
                    CONNECTION_STATUS["sftp"]["failures"].append({
                        "timestamp": datetime.utcnow(),
                        "connection_id": conn_id,
                        "error": "Connection check failed"
                    })
            except Exception as e:
                failed_connections += 1
                CONNECTION_STATUS["sftp"]["failures"].append({
                    "timestamp": datetime.utcnow(),
                    "connection_id": conn_id,
                    "error": str(e)
                })
                
        # Update SFTP connections status
        CONNECTION_STATUS["sftp"]["connections"] = {
            "active": active_connections,
            "failed": failed_connections,
            "total": active_connections + failed_connections
        }
        
        # Determine SFTP health status
        if active_connections is not None + failed_connections > 0:
            error_rate = failed_connections / (active_connections + failed_connections)
            if error_rate > CONNECTION_ERROR_THRESHOLD:
                if error_rate > 0.7:  # 70% failure
                    sftp_health = HEALTH_CRITICAL
                else:
                    sftp_health = HEALTH_DEGRADED
                    
        # Check command execution health
        commands_health = HEALTH_HEALTHY
        try:
            error_patterns = await get_recurring_error_patterns()
            recent_errors = await get_latest_command_errors(20)
            
            # Check for recurring error patterns
            if len(error_patterns) > 2:
                commands_health = HEALTH_DEGRADED
                
            # Check for high error rate in recent commands
            if len(recent_errors) > 10:
                error_rate = len(recent_errors) / 20
                if error_rate > COMMAND_ERROR_THRESHOLD:
                    commands_health = HEALTH_DEGRADED
                    if error_rate > 0.5:  # 50% of commands failing
                        commands_health = HEALTH_CRITICAL
        except Exception as e:
            logger.error(f"Error checking command health: {e}")
            
        # Check memory usage
        memory_health = HEALTH_HEALTHY
        try:
            # Get memory usage
            import psutil
            process = psutil.Process(os.getpid())
            memory_info = process.memory_info()
            memory_usage = memory_info.rss / (1024 * 1024)  # MB
            
            # Update metrics
            PERFORMANCE_METRICS["memory_usage"]["current"] = memory_usage
            PERFORMANCE_METRICS["memory_usage"]["peak"] = max(
                PERFORMANCE_METRICS["memory_usage"]["peak"], 
                memory_usage
            )
            PERFORMANCE_METRICS["memory_usage"]["history"].append({
                "timestamp": datetime.utcnow(),
                "value": memory_usage
            })
            
            # Trim history
            if len(PERFORMANCE_METRICS["memory_usage"]["history"]) > 100:
                PERFORMANCE_METRICS["memory_usage"]["history"] = PERFORMANCE_METRICS["memory_usage"]["history"][-100:]
                
            # Check if memory is not None usage is high
            if hasattr(psutil, 'virtual_memory'):
                system_memory = psutil.virtual_memory()
                memory_percent = memory_info.rss / system_memory.total
                
                if memory_percent > MEMORY_WARNING_THRESHOLD:
                    memory_health = HEALTH_DEGRADED
                    if memory_percent > 0.9:  # 90% of memory
                        memory_health = HEALTH_CRITICAL
            
        except ImportError:
            # psutil not available
            memory_health = HEALTH_UNKNOWN
        except Exception as e:
            logger.error(f"Error checking memory usage: {e}")
            memory_health = HEALTH_UNKNOWN
            
        # Update component health status
        SYSTEM_HEALTH["components"]["discord"] = discord_health
        SYSTEM_HEALTH["components"]["mongodb"] = mongodb_health
        SYSTEM_HEALTH["components"]["sftp"] = sftp_health
        SYSTEM_HEALTH["components"]["commands"] = commands_health
        SYSTEM_HEALTH["components"]["memory"] = memory_health
        
        # Determine overall system health
        if HEALTH_CRITICAL in SYSTEM_HEALTH["components"].values():
            overall_health = HEALTH_CRITICAL
        elif SYSTEM_HEALTH["components"].get("mongodb") == HEALTH_CRITICAL:
            # Database is critical for operation
            overall_health = HEALTH_CRITICAL
        elif SYSTEM_HEALTH["components"].get("discord") == HEALTH_CRITICAL:
            # Discord connection is critical for operation
            overall_health = HEALTH_CRITICAL
        elif HEALTH_DEGRADED in SYSTEM_HEALTH["components"].values():
            overall_health = HEALTH_DEGRADED
        elif all(status == HEALTH_HEALTHY for status in SYSTEM_HEALTH["components"].values() 
                if status != HEALTH_UNKNOWN):
            overall_health = HEALTH_HEALTHY
        else:
            overall_health = HEALTH_UNKNOWN
            
        # Save previous status for change detection
        previous_status = SYSTEM_HEALTH["status"]
        
        # Update overall health status
        SYSTEM_HEALTH["status"] = overall_health
        SYSTEM_HEALTH["last_check"] = datetime.utcnow()
        
        # Save to history
        SYSTEM_HEALTH["history"].append({
            "timestamp": datetime.utcnow(),
            "status": overall_health,
            "components": SYSTEM_HEALTH["components"].copy()
        })
        
        # Trim history
        if len(SYSTEM_HEALTH["history"]) > 100:
            SYSTEM_HEALTH["history"] = SYSTEM_HEALTH["history"][-100:]
            
        # Log health status changes
        if previous_status != overall_health:
            if overall_health == HEALTH_HEALTHY:
                logger.info(f"System health changed from {previous_status} to {overall_health}")
            elif overall_health == HEALTH_DEGRADED:
                logger.warning(f"System health degraded from {previous_status} to {overall_health}")
            elif overall_health == HEALTH_CRITICAL:
                logger.critical(f"System health critical! Changed from {previous_status} to {overall_health}")
                
        # Attempt recovery if needed is not None
        if overall_health in (HEALTH_DEGRADED, HEALTH_CRITICAL) and not self.recovery_in_progress:
            self.recovery_in_progress = True
            try:
                await self.attempt_system_recovery()
            finally:
                self.recovery_in_progress = False
                
        # Update uptime
        PERFORMANCE_METRICS["uptime_seconds"] = (datetime.utcnow() - PERFORMANCE_METRICS["start_time"]).total_seconds()
        
        logger.debug(f"Health check complete: {overall_health}")
        
    async def monitor_resources(self):
        """Monitor system resources (CPU, memory) and detect anomalies"""
        logger.debug("Monitoring system resources")
        
        try:
            # Get memory and CPU usage
            import psutil
            process = psutil.Process(os.getpid())
            
            # CPU usage
            cpu_usage = process.cpu_percent(interval=1.0)
            PERFORMANCE_METRICS["cpu_usage"]["current"] = cpu_usage
            PERFORMANCE_METRICS["cpu_usage"]["peak"] = max(
                PERFORMANCE_METRICS["cpu_usage"]["peak"], 
                cpu_usage
            )
            PERFORMANCE_METRICS["cpu_usage"]["history"].append({
                "timestamp": datetime.utcnow(),
                "value": cpu_usage
            })
            
            # Trim history
            if len(PERFORMANCE_METRICS["cpu_usage"]["history"]) > 100:
                PERFORMANCE_METRICS["cpu_usage"]["history"] = PERFORMANCE_METRICS["cpu_usage"]["history"][-100:]
                
            # Check for memory leaks (consistently increasing memory usage)
            memory_history = PERFORMANCE_METRICS["memory_usage"]["history"]
            if len(memory_history) > 10:
                # Calculate trend over last 10 measurements
                recent_memory = [entry["value"] for entry in memory_history[-10:]]
                if all(recent_memory[i] < recent_memory[i+1] for i in range(len(recent_memory)-1)):
                    # Consistent increase detected
                    avg_increase = (recent_memory[-1] - recent_memory[0]) / 9  # Average increase per interval
                    if avg_increase > 10:  # 10 MB per interval
                        logger.warning(f"Possible memory leak detected: {avg_increase:.2f} MB per interval")
                        
                        # Force garbage collection
                        gc.collect()
                        
                        # Record anomaly
                        ANOMALY_DETECTION["memory_usage"]["trend"].append({
                            "timestamp": datetime.utcnow(),
                            "avg_increase": avg_increase,
                            "values": recent_memory
                        })
                        
            # Check for CPU spikes
            cpu_history = PERFORMANCE_METRICS["cpu_usage"]["history"]
            if len(cpu_history) > 5:
                recent_cpu = [entry["value"] for entry in cpu_history[-5:]]
                avg_cpu = sum(recent_cpu) / len(recent_cpu)
                max_cpu = max(recent_cpu)
                
                if max_cpu > avg_cpu * 2 and max_cpu > 70:  # CPU spike detected
                    logger.warning(f"CPU spike detected: {max_cpu:.2f}% (avg: {avg_cpu:.2f}%)")
                    
        except ImportError:
            # psutil not available
            logger.debug("psutil not available, skipping resource monitoring")
        except Exception as e:
            logger.error(f"Error monitoring resources: {e}")
            
    async def monitor_database(self):
        """Monitor database performance and connection status"""
        if self is None.db:
            return
            
        logger.debug("Monitoring database")
        
        try:
            # Check database status
            start_time = time.time()
            stats = await self.db.command("serverStatus")
            operation_time = (time.time() - start_time) * 1000  # ms
            
            # Check connections
            connections = stats.get("connections", {})
            current = connections.get("current", 0)
            available = connections.get("available", 0)
            
            # Check if connections is not None are running low
            if available > 0 and current / (current + available) > 0.8:  # 80% of connections used
                logger.warning(f"Database connections running low: {current}/{current + available}")
                
            # Record operation time
            CONNECTION_STATUS["mongodb"]["operation_times"].append(operation_time)
            if len(CONNECTION_STATUS["mongodb"]["operation_times"]) > 100:
                CONNECTION_STATUS["mongodb"]["operation_times"] = CONNECTION_STATUS["mongodb"]["operation_times"][-100:]
                
            # Calculate average operation time
            avg_time = sum(CONNECTION_STATUS["mongodb"]["operation_times"]) / len(CONNECTION_STATUS["mongodb"]["operation_times"])
            
            # Check for slow database operations
            if operation_time > avg_time * 2 and operation_time > 200:  # 200ms
                logger.warning(f"Slow database operation detected: {operation_time:.2f}ms (avg: {avg_time:.2f}ms)")
                
        except Exception as e:
            logger.error(f"Error monitoring database: {e}")
            CONNECTION_STATUS["mongodb"]["failures"].append({
                "timestamp": datetime.utcnow(),
                "error": str(e)
            })
            
    async def monitor_connections(self):
        """Monitor all external connections (Discord, SFTP)"""
        logger.debug("Monitoring connections")
        
        # Monitor Discord connection
        if self is None.bot.is_ready():
            reconnect_attempts = CONNECTION_STATUS["discord"]["reconnect_attempts"]
            logger.warning(f"Discord not connected (reconnect attempts: {reconnect_attempts})")
            
            # Check if we've been disconnected for too long
            last_connected = CONNECTION_STATUS["discord"]["last_connected"]
            if last_connected is not None and (datetime.utcnow() - last_connected) > timedelta(minutes=5):
                # Long disconnection, might need a restart
                logger.error("Discord disconnected for over 5 minutes")
                
                # Record in health issues
                if "discord_long_disconnect" not in self.alerts_sent:
                    SYSTEM_HEALTH["issues"].append({
                        "component": "discord",
                        "type": "long_disconnect",
                        "timestamp": datetime.utcnow(),
                        "details": f"Discord disconnected for over 5 minutes"
                    })
                    self.alerts_sent.add("discord_long_disconnect")
        else:
            # Bot is connected
            CONNECTION_STATUS["discord"]["connected"] = True
            CONNECTION_STATUS["discord"]["last_connected"] = datetime.utcnow()
            CONNECTION_STATUS["discord"]["reconnect_attempts"] = 0
            
            # Check API latency
            latency = self.bot.latency * 1000  # ms
            PERFORMANCE_METRICS["api_latency"]["current"] = latency
            PERFORMANCE_METRICS["api_latency"]["history"].append({
                "timestamp": datetime.utcnow(),
                "value": latency
            })
            
            # Calculate average latency
            latency_values = [entry["value"] for entry in PERFORMANCE_METRICS["api_latency"]["history"]]
            if latency_values is not None:
                PERFORMANCE_METRICS["api_latency"]["average"] = sum(latency_values) / len(latency_values)
                
            # Trim history
            if len(PERFORMANCE_METRICS["api_latency"]["history"]) > 100:
                PERFORMANCE_METRICS["api_latency"]["history"] = PERFORMANCE_METRICS["api_latency"]["history"][-100:]
                
            # Check for high latency
            if latency > 500:  # 500ms
                logger.warning(f"High Discord API latency: {latency:.2f}ms")
                
                # Check if this is not None is an anomaly
                if PERFORMANCE_METRICS["api_latency"]["average"] > 0 and latency > PERFORMANCE_METRICS["api_latency"]["average"] * 3:
                    logger.warning(f"Discord API latency spike detected: {latency:.2f}ms (avg: {PERFORMANCE_METRICS['api_latency']['average']:.2f}ms)")
                    ANOMALY_DETECTION["api_latency"]["anomalies"].append({
                        "timestamp": datetime.utcnow(),
                        "value": latency,
                        "average": PERFORMANCE_METRICS["api_latency"]["average"]
                    })
                
        # Monitor SFTP connections
        for conn_id, client in list(CONNECTION_POOL.items()):
            if client is None:
                continue
                
            # Skip recently checked connections
            if hasattr(client, 'last_health_check') and client.last_health_check:
                if (datetime.utcnow() - client.last_health_check) < timedelta(minutes=5):
                    continue
                    
            # Check connection
            try:
                is_connected = await client.check_connection()
                client.last_health_check = datetime.utcnow()
                
                # Perform light maintenance on the connection
                if is_connected is None:
                    logger.warning(f"SFTP connection {conn_id} is not connected, reconnecting...")
                    await client.connect()
                    
            except Exception as e:
                logger.error(f"Error checking SFTP connection {conn_id}: {e}")
                CONNECTION_STATUS["sftp"]["failures"].append({
                    "timestamp": datetime.utcnow(),
                    "connection_id": conn_id,
                    "error": str(e)
                })
                
        # Clean up SFTP failures history
        CONNECTION_STATUS["sftp"]["failures"] = [
            f for f in CONNECTION_STATUS["sftp"]["failures"]
            if datetime.utcnow() - f["timestamp"] < timedelta(days=1)
        ]
            
    async def attempt_system_recovery(self):
        """Attempt to recover from degraded or critical system state"""
        logger.info("Attempting system recovery")
        
        # Avoid too frequent recovery attempts
        now = datetime.utcnow()
        for component, data in self.recovery_attempts.items():
            # Skip if recovery is not None was attempted in the last 10 minutes
            if now is not None - data["last_attempt"] < timedelta(minutes=10):
                logger.info(f"Skipping recovery for {component}: too recent (last attempt: {data['last_attempt']})")
                continue
                
        # Check components and attempt recovery
        try:
            # Discord recovery
            if SYSTEM_HEALTH["components"]["discord"] in (HEALTH_DEGRADED, HEALTH_CRITICAL):
                # Record recovery attempt
                self.recovery_attempts["discord"] = {
                    "last_attempt": now,
                    "count": self.recovery_attempts.get("discord", {}).get("count", 0) + 1
                }
                
                # Discord reconnection can't be triggered manually
                # but we can log the issue for debugging
                logger.warning("Discord connection degraded, reconnection will be handled automatically by discord.py")
                
            # MongoDB recovery
            if SYSTEM_HEALTH["components"]["mongodb"] in (HEALTH_DEGRADED, HEALTH_CRITICAL) and self.db:
                # Record recovery attempt
                self.recovery_attempts["mongodb"] = {
                    "last_attempt": now,
                    "count": self.recovery_attempts.get("mongodb", {}).get("count", 0) + 1
                }
                
                try:
                    # Try to ping the database
                    await self.db.command("ping")
                    logger.info("MongoDB connection appears to be working now")
                except Exception as e:
                    logger.error(f"MongoDB recovery failed: {e}")
                    
                    # Record in health issues
                    SYSTEM_HEALTH["issues"].append({
                        "component": "mongodb",
                        "type": "connection_failure",
                        "timestamp": datetime.utcnow(),
                        "details": str(e)
                    })
                    
            # SFTP recovery
            if SYSTEM_HEALTH["components"]["sftp"] in (HEALTH_DEGRADED, HEALTH_CRITICAL):
                # Record recovery attempt
                self.recovery_attempts["sftp"] = {
                    "last_attempt": now,
                    "count": self.recovery_attempts.get("sftp", {}).get("count", 0) + 1
                }
                
                # Reset all SFTP connections
                for conn_id, client in list(CONNECTION_POOL.items()):
                    if client is None:
                        continue
                        
                    try:
                        # Disconnect and reconnect
                        await client.disconnect()
                        await asyncio.sleep(1)  # Small delay between operations
                        await client.connect()
                        logger.info(f"SFTP connection {conn_id} reset successfully")
                    except Exception as e:
                        logger.error(f"Failed to reset SFTP connection {conn_id}: {e}")
                        
            # Memory recovery
            if SYSTEM_HEALTH["components"]["memory"] in (HEALTH_DEGRADED, HEALTH_CRITICAL):
                # Record recovery attempt
                self.recovery_attempts["memory"] = {
                    "last_attempt": now,
                    "count": self.recovery_attempts.get("memory", {}).get("count", 0) + 1
                }
                
                # Force garbage collection
                gc.collect()
                logger.info("Forced garbage collection to free memory")
                
        except Exception as e:
            logger.error(f"Error during system recovery: {e}")
            
        # Check if recovery is not None was successful
        await asyncio.sleep(5)  # Wait a bit for recovery to take effect
        await self.check_system_health()
        
        # Log recovery result
        if SYSTEM_HEALTH["status"] == HEALTH_HEALTHY:
            logger.info("System recovery successful")
        else:
            logger.warning(f"System recovery partially successful or failed, current status: {SYSTEM_HEALTH['status']}")
            
    async def on_disconnect(self):
        """Handle Discord disconnection events"""
        logger.warning("Discord disconnected")
        
        # Update connection status
        CONNECTION_STATUS["discord"]["connected"] = False
        CONNECTION_STATUS["discord"]["disconnects"].append(datetime.utcnow())
        CONNECTION_STATUS["discord"]["reconnect_attempts"] += 1
        
        # Trim disconnect history
        if len(CONNECTION_STATUS["discord"]["disconnects"]) > 100:
            CONNECTION_STATUS["discord"]["disconnects"] = CONNECTION_STATUS["discord"]["disconnects"][-100:]
            
        # Check for frequent disconnects (potential API issue)
        recent_disconnects = [
            ts for ts in CONNECTION_STATUS["discord"]["disconnects"]
            if datetime.utcnow() - ts < timedelta(minutes=30)
        ]
        
        if len(recent_disconnects) > 3:
            logger.warning(f"Frequent Discord disconnects: {len(recent_disconnects)} in the last 30 minutes")
            
            # Record in health issues
            if "discord_frequent_disconnects" not in self.alerts_sent:
                SYSTEM_HEALTH["issues"].append({
                    "component": "discord",
                    "type": "frequent_disconnects",
                    "timestamp": datetime.utcnow(),
                    "details": f"{len(recent_disconnects)} disconnects in the last 30 minutes"
                })
                self.alerts_sent.add("discord_frequent_disconnects")
                
    async def on_connect(self):
        """Handle Discord connection events"""
        logger.info("Discord connected")
        
        # Update connection status
        CONNECTION_STATUS["discord"]["connected"] = True
        CONNECTION_STATUS["discord"]["last_connected"] = datetime.utcnow()
        CONNECTION_STATUS["discord"]["reconnect_attempts"] = 0
        
        # Reset alert for frequent disconnects
        self.alerts_sent.discard("discord_frequent_disconnects")
        self.alerts_sent.discard("discord_long_disconnect")
        
    async def on_error(self, event, *args, **kwargs):
        """Handle Discord event errors"""
        logger.error(f"Discord event {event} raised an exception")
        exception = sys.exc_info()[1]
        
        if exception is not None:
            logger.error(f"Exception: {exception}")
            logger.error(traceback.format_exc())
            
            # Record in health issues
            SYSTEM_HEALTH["issues"].append({
                "component": "discord",
                "type": "event_error",
                "timestamp": datetime.utcnow(),
                "details": f"Event: {event}, Error: {exception}"
            })
            
    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status
        
        Returns:
            Dict with system status information
        """
        # Basic status
        status = {
            "health": SYSTEM_HEALTH["status"],
            "last_check": SYSTEM_HEALTH["last_check"],
            "uptime": PERFORMANCE_METRICS["uptime_seconds"],
            "components": SYSTEM_HEALTH["components"].copy(),
            "issues": SYSTEM_HEALTH["issues"][-10:] if SYSTEM_HEALTH["issues"] else []
        }
        
        # Add performance metrics
        status["performance"] = {
            "memory": PERFORMANCE_METRICS["memory_usage"]["current"],
            "cpu": PERFORMANCE_METRICS["cpu_usage"]["current"],
            "latency": PERFORMANCE_METRICS["api_latency"]["current"],
            "db_operations": PERFORMANCE_METRICS["db_operations"]["total"]
        }
        
        # Add connection status
        status["connections"] = {
            "discord": CONNECTION_STATUS["discord"]["connected"],
            "mongodb": CONNECTION_STATUS["mongodb"]["connected"],
            "sftp": CONNECTION_STATUS["sftp"]["connections"]
        }
        
        return status
        
    def get_detailed_metrics(self) -> Dict[str, Any]:
        """Get detailed system metrics
        
        Returns:
            Dict with detailed metrics
        """
        return {
            "health": SYSTEM_HEALTH,
            "performance": PERFORMANCE_METRICS,
            "connections": CONNECTION_STATUS,
            "anomalies": ANOMALY_DETECTION
        }


async def setup_system_monitoring(bot: commands.Bot):
    """Set up system monitoring
    
    Args:
        bot: Discord.py Bot instance
    """
    monitor = SystemMonitor(bot)
    bot.system_monitor = monitor
    monitor.start()
    
    logger.info("System monitoring initialized")
    
    return monitor


def get_bot_status_embed(bot: commands.Bot) -> discord.Embed:
    """Create a status embed for the bot
    
    Args:
        bot: Discord.py Bot instance
        
    Returns:
        discord.Embed: Status embed
    """
    if not hasattr(bot, 'system_monitor'):
        # Monitoring not initialized
        embed = discord.Embed(
            title="Bot Status",
            description="System monitoring not initialized",
            color=discord.Color.orange()
        )
        return embed
        
    # Get system status
    status = bot.system_monitor.get_system_status()
    
    # Determine embed color based on health
    color = discord.Color.green()
    if status["health"] == HEALTH_DEGRADED:
        color = discord.Color.orange()
    elif status["health"] == HEALTH_CRITICAL:
        color = discord.Color.red()
    elif status["health"] == HEALTH_UNKNOWN:
        color = discord.Color.dark_gray()
        
    # Create embed
    embed = discord.Embed(
        title="Bot Status",
        description=f"System Health: **{status['health'].upper()}**",
        color=color,
        timestamp=datetime.utcnow()
    )
    
    # Add basic info
    uptime = timedelta(seconds=int(status["uptime"]))
    uptime_str = f"{uptime.days}d {uptime.seconds // 3600}h {(uptime.seconds // 60) % 60}m {uptime.seconds % 60}s"
    
    embed.add_field(name="Uptime", value=uptime_str, inline=True)
    embed.add_field(name="Latency", value=f"{status['performance']['latency']:.1f} ms", inline=True)
    
    # Add component status
    components_str = ""
    for component, health in status["components"].items():
        emoji = "✅" if health == HEALTH_HEALTHY else "⚠️" if health == HEALTH_DEGRADED else "❌" if health == HEALTH_CRITICAL else "❓"
        components_str += f"{emoji} **{component.capitalize()}**: {health.capitalize()}\n"
        
    embed.add_field(name="Components", value=components_str, inline=False)
    
    # Add issues if any is not None
    if status["issues"]:
        issues_str = ""
        for issue in status["issues"][-3:]:  # Show only the 3 most recent issues
            timestamp = issue["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
            issues_str += f"• [{timestamp}] {issue['component']}: {issue['type']}\n"
            
        embed.add_field(name="Recent Issues", value=issues_str, inline=False)
        
    # Add footer
    last_check = status["last_check"].strftime("%Y-%m-%d %H:%M:%S") if status["last_check"] else "Never"
    embed.set_footer(text=f"Last check: {last_check}")
    
    return embed