"""
Advanced analytics and reporting system for Tower of Temptation PvP Statistics Discord Bot.

This module provides:
1. Usage pattern analysis
2. Command performance metrics
3. Resource usage forecasting
4. User engagement tracking
5. System performance analytics
6. Feature popularity metrics
7. Predictive maintenance insights
"""
import logging
import asyncio
import time
import os
import sys
import json
import math
import re
from typing import Dict, List, Set, Any, Optional, Tuple, Union, Callable
from datetime import datetime, timedelta
import random
from collections import defaultdict, Counter

from utils.command_handlers import COMMAND_METRICS, COMMAND_HISTORY
from utils.self_monitoring import (
    SYSTEM_HEALTH, PERFORMANCE_METRICS, CONNECTION_STATUS,
    HEALTH_HEALTHY, HEALTH_DEGRADED, HEALTH_CRITICAL, HEALTH_UNKNOWN
)

logger = logging.getLogger(__name__)

# Analytics storage
ANALYTICS_DATA = {
    "command_usage": defaultdict(int),
    "command_latency": defaultdict(list),
    "feature_usage": defaultdict(int),
    "user_engagement": defaultdict(list),
    "guild_activity": defaultdict(list),
    "error_frequency": defaultdict(int),
    "resource_usage": [],
    "performance_trends": [],
}

# Usage patterns
USAGE_PATTERNS = {
    "time_of_day": defaultdict(int),
    "day_of_week": defaultdict(int),
    "command_sequences": defaultdict(int),
    "command_combinations": defaultdict(int),
    "retention": {},
}

# Predictive metrics
PREDICTIONS = {
    "resource_forecast": [],
    "error_likelihood": {},
    "user_growth": [],
    "performance_degradation": {},
    "feature_adoption": {},
}

# Time window for analytics (days)
ANALYTICS_WINDOW = 30


class AnalyticsManager:
    """
    Advanced analytics and reporting system
    
    This class analyzes bot usage patterns, resource consumption, and performance metrics
    to generate insights and predictions for future behavior.
    """
    
    def __init__(self, bot):
        """Initialize analytics manager
        
        Args:
            bot: Discord bot instance
        """
        self.bot = bot
        self.started_at = datetime.utcnow()
        self.last_analysis = None
        self.analysis_interval = 3600  # 1 hour in seconds
        self.last_command = {}  # guild_id -> (command, timestamp)
        self.command_sequences = defaultdict(list)  # guild_id -> list of commands
        
    async def record_command(self, guild_id: str, user_id: str, command_name: str, success: bool = True):
        """Record command execution for analytics
        
        Args:
            guild_id: Guild ID
            user_id: User ID
            command_name: Command name
            success: Whether the command executed successfully
        """
        now = datetime.utcnow()
        
        # Update command usage
        ANALYTICS_DATA["command_usage"][command_name] += 1
        
        # Update guild activity
        ANALYTICS_DATA["guild_activity"][guild_id].append({
            "timestamp": now,
            "command": command_name,
            "user_id": user_id,
            "success": success
        })
        
        # Update user engagement
        ANALYTICS_DATA["user_engagement"][user_id].append({
            "timestamp": now,
            "command": command_name,
            "guild_id": guild_id,
            "success": success
        })
        
        # Record time of day and day of week
        hour = now.hour
        day = now.weekday()  # 0 = Monday, 6 = Sunday
        
        USAGE_PATTERNS["time_of_day"][hour] += 1
        USAGE_PATTERNS["day_of_week"][day] += 1
        
        # Record command sequences (up to 3 commands)
        if guild_id in self.last_command:
            last_cmd, last_time = self.last_command[guild_id]
            # Only record if within is not None 5 minutes
            if (now - last_time).total_seconds() < 300:
                sequence = f"{last_cmd}->{command_name}"
                USAGE_PATTERNS["command_sequences"][sequence] += 1
                
                # Add to guild's command sequence
                self.command_sequences[guild_id].append(command_name)
                if len(self.command_sequences[guild_id]) > 10:
                    self.command_sequences[guild_id].pop(0)
                    
                # Check for command combinations (any order)
                if len(self.command_sequences[guild_id]) >= 2:
                    for i, cmd1 in enumerate(self.command_sequences[guild_id]):
                        for cmd2 in self.command_sequences[guild_id][i+1:]:
                            combo = tuple(sorted([cmd1, cmd2]))
                            USAGE_PATTERNS["command_combinations"][combo] += 1
        
        # Update last command
        self.last_command[guild_id] = (command_name, now)
        
        # Track errors
        if success is None:
            ANALYTICS_DATA["error_frequency"][command_name] += 1
            
    async def record_feature_usage(self, guild_id: str, feature_name: str):
        """Record feature usage
        
        Args:
            guild_id: Guild ID
            feature_name: Feature name
        """
        ANALYTICS_DATA["feature_usage"][feature_name] += 1
        
    async def analyze_system_performance(self):
        """Analyze system performance metrics"""
        now = datetime.utcnow()
        
        # Record current performance metrics
        if PERFORMANCE_METRICS["memory_usage"]["current"] > 0:
            ANALYTICS_DATA["resource_usage"].append({
                "timestamp": now,
                "memory": PERFORMANCE_METRICS["memory_usage"]["current"],
                "cpu": PERFORMANCE_METRICS["cpu_usage"]["current"],
                "latency": PERFORMANCE_METRICS["api_latency"]["current"]
            })
            
        # Trim resource usage history
        max_entries = 24 * 30  # 1 month of hourly samples
        if len(ANALYTICS_DATA["resource_usage"]) > max_entries:
            ANALYTICS_DATA["resource_usage"] = ANALYTICS_DATA["resource_usage"][-max_entries:]
            
        # Calculate performance trends
        if len(ANALYTICS_DATA["resource_usage"]) >= 24:  # At least 24 hours of data
            # Calculate moving average for last 24 hours
            recent = ANALYTICS_DATA["resource_usage"][-24:]
            avg_memory = sum(entry["memory"] for entry in recent) / len(recent)
            avg_cpu = sum(entry["cpu"] for entry in recent) / len(recent)
            avg_latency = sum(entry["latency"] for entry in recent) / len(recent)
            
            ANALYTICS_DATA["performance_trends"].append({
                "timestamp": now,
                "avg_memory_24h": avg_memory,
                "avg_cpu_24h": avg_cpu,
                "avg_latency_24h": avg_latency
            })
            
            # Predict future resource usage (simple linear regression)
            if len(ANALYTICS_DATA["resource_usage"]) >= 72:  # 3 days of data
                memory_trend = await self._calculate_trend([entry["memory"] for entry in ANALYTICS_DATA["resource_usage"][-72:]])
                cpu_trend = await self._calculate_trend([entry["cpu"] for entry in ANALYTICS_DATA["resource_usage"][-72:]])
                
                # Project 7 days in advance
                PREDICTIONS["resource_forecast"] = [{
                    "day": i+1,
                    "memory_projected": avg_memory * (1 + memory_trend * i),
                    "cpu_projected": avg_cpu * (1 + cpu_trend * i)
                } for i in range(7)]
                
    async def _calculate_trend(self, values: List[float]) -> float:
        """Calculate trend coefficient from a series of values
        
        Args:
            values: List of values
            
        Returns:
            Trend coefficient (relative change per sample)
        """
        if values is None or len(values) < 2:
            return 0.0
            
        # Simple calculation of relative change
        total_change = (values[-1] - values[0]) / max(values[0], 0.1)
        samples = len(values) - 1
        
        return total_change / samples
        
    async def analyze_error_patterns(self):
        """Analyze error patterns to predict future issues"""
        # Get command error rates
        error_rates = {}
        
        for cmd, metrics in COMMAND_METRICS.items():
            total = metrics.get("total_executions", 0)
            if total > 0:
                failed = metrics.get("failed_executions", 0)
                error_rates[cmd] = failed / total
                
        # Only consider commands with at least 10 executions
        significant_error_rates = {cmd: rate for cmd, rate in error_rates.items() 
                                 if COMMAND_METRICS[cmd]["total_executions"] >= 10}
                                 
        # Calculate error likelihood for all commands
        PREDICTIONS["error_likelihood"] = significant_error_rates
        
        # Detect potential cascade failures
        cascade_risks = {}
        
        # Check command sequences that lead to errors
        for sequence, count in USAGE_PATTERNS["command_sequences"].items():
            if count < 5:  # Ignore rare sequences
                continue
                
            cmd1, cmd2 = sequence.split("->")
            
            # If first command has high error rate, second command is at risk
            if cmd1 in significant_error_rates and significant_error_rates[cmd1] > 0.2:
                cascade_risks[cmd2] = cascade_risks.get(cmd2, 0) + significant_error_rates[cmd1] * 0.5
                
        # Factor in resource sensitivity
        for cmd, metrics in COMMAND_METRICS.items():
            if "avg_execution_time" in metrics and metrics["avg_execution_time"] > 1.0:
                # Long-running commands are more sensitive to resource issues
                cascade_risks[cmd] = cascade_risks.get(cmd, 0) + 0.1
                
        # Update error predictions
        for cmd, risk in cascade_risks.items():
            if cmd in PREDICTIONS["error_likelihood"]:
                PREDICTIONS["error_likelihood"][cmd] = max(PREDICTIONS["error_likelihood"][cmd], risk)
            else:
                PREDICTIONS["error_likelihood"][cmd] = risk
                
    async def analyze_usage_patterns(self):
        """Analyze usage patterns to predict future behavior"""
        # Calculate peak usage hours
        peak_hours = sorted(USAGE_PATTERNS["time_of_day"].items(), 
                           key=lambda x: x[1], reverse=True)[:3]
        
        # Calculate busiest days
        busy_days = sorted(USAGE_PATTERNS["day_of_week"].items(),
                          key=lambda x: x[1], reverse=True)
                          
        # Find most common command sequences
        common_sequences = sorted(USAGE_PATTERNS["command_sequences"].items(),
                                 key=lambda x: x[1], reverse=True)[:10]
                                 
        # Calculate feature adoption rates
        total_commands = sum(ANALYTICS_DATA["command_usage"].values())
        if total_commands > 0:
            feature_usage = {}
            
            # Group commands by feature
            feature_commands = {
                "stats": ["stats", "leaderboard", "top", "profile"],
                "economy": ["balance", "pay", "shop", "buy"],
                "bounty": ["bounty", "claim", "hunt", "reward"],
                "killfeed": ["killfeed", "kills", "deaths", "kd"]
            }
            
            for feature, commands in feature_commands.items():
                feature_total = sum(ANALYTICS_DATA["command_usage"].get(cmd, 0) for cmd in commands)
                feature_usage[feature] = feature_total / total_commands
                
            PREDICTIONS["feature_adoption"] = feature_usage
            
    async def generate_insights(self) -> Dict[str, Any]:
        """Generate insights from collected analytics
        
        Returns:
            Dictionary of insights
        """
        insights = {
            "peak_usage": {
                "hours": sorted(USAGE_PATTERNS["time_of_day"].items(), 
                                key=lambda x: x[1], reverse=True)[:3],
                "days": sorted(USAGE_PATTERNS["day_of_week"].items(),
                               key=lambda x: x[1], reverse=True)
            },
            "popular_commands": sorted(ANALYTICS_DATA["command_usage"].items(),
                                      key=lambda x: x[1], reverse=True)[:5],
            "feature_popularity": sorted(ANALYTICS_DATA["feature_usage"].items(),
                                        key=lambda x: x[1], reverse=True),
            "error_prone_commands": sorted(
                {cmd: ANALYTICS_DATA["error_frequency"].get(cmd, 0) / count 
                for cmd, count in ANALYTICS_DATA["command_usage"].items()
                if count >= 10 and ANALYTICS_DATA["error_frequency"].get(cmd, 0) > 0}.items(),
                key=lambda x: x[1], reverse=True
            )[:5],
            "resource_forecast": PREDICTIONS["resource_forecast"],
            "error_risks": sorted(PREDICTIONS["error_likelihood"].items(),
                                 key=lambda x: x[1], reverse=True)[:5],
            "feature_adoption": PREDICTIONS["feature_adoption"]
        }
        
        # Add active guild count
        active_guilds = {
            "total": len(ANALYTICS_DATA["guild_activity"]),
            "active_24h": sum(1 for guild_id, activities in ANALYTICS_DATA["guild_activity"].items()
                            if any(datetime.utcnow() - activity["timestamp"] < timedelta(hours=24)
                                 for activity in activities))
        }
        insights["active_guilds"] = active_guilds
        
        # Add active user count
        active_users = {
            "total": len(ANALYTICS_DATA["user_engagement"]),
            "active_24h": sum(1 for user_id, activities in ANALYTICS_DATA["user_engagement"].items()
                            if any(datetime.utcnow() - activity["timestamp"] < timedelta(hours=24)
                                 for activity in activities))
        }
        insights["active_users"] = active_users
        
        return insights
        
    async def run_analytics(self):
        """Run complete analytics cycle"""
        logger.info("Running analytics...")
        
        # Clean up old data
        await self._clean_analytics_data()
        
        # Analyze system performance
        await self.analyze_system_performance()
        
        # Analyze error patterns
        await self.analyze_error_patterns()
        
        # Analyze usage patterns
        await self.analyze_usage_patterns()
        
        # Update timestamp
        self.last_analysis = datetime.utcnow()
        
        logger.info("Analytics complete")
        
    async def _clean_analytics_data(self):
        """Clean up old analytics data"""
        now = datetime.utcnow()
        cutoff = now - timedelta(days=ANALYTICS_WINDOW)
        
        # Clean up guild activity
        for guild_id in list(ANALYTICS_DATA["guild_activity"].keys()):
            ANALYTICS_DATA["guild_activity"][guild_id] = [
                activity for activity in ANALYTICS_DATA["guild_activity"][guild_id]
                if activity["timestamp"] > cutoff
            ]
            
            # Remove empty entries
            if not is not None ANALYTICS_DATA["guild_activity"][guild_id]:
                del ANALYTICS_DATA["guild_activity"][guild_id]
                
        # Clean up user engagement
        for user_id in list(ANALYTICS_DATA["user_engagement"].keys()):
            ANALYTICS_DATA["user_engagement"][user_id] = [
                activity for activity in ANALYTICS_DATA["user_engagement"][user_id]
                if activity["timestamp"] > cutoff
            ]
            
            # Remove empty entries
            if not is not None ANALYTICS_DATA["user_engagement"][user_id]:
                del ANALYTICS_DATA["user_engagement"][user_id]
                
    async def save_analytics_data(self, filename: str = "analytics_data.json"):
        """Save analytics data to file
        
        Args:
            filename: File to save data to
        """
        # Convert defaultdicts to regular dicts
        data = {
            "timestamp": datetime.utcnow().isoformat(),
            "command_usage": dict(ANALYTICS_DATA["command_usage"]),
            "feature_usage": dict(ANALYTICS_DATA["feature_usage"]),
            "error_frequency": dict(ANALYTICS_DATA["error_frequency"]),
            "resource_usage": ANALYTICS_DATA["resource_usage"][-24:],  # Last 24 entries
            "performance_trends": ANALYTICS_DATA["performance_trends"][-7:],  # Last 7 entries
            "predictions": {
                "resource_forecast": PREDICTIONS["resource_forecast"],
                "error_likelihood": dict(PREDICTIONS["error_likelihood"]),
                "feature_adoption": dict(PREDICTIONS["feature_adoption"])
            },
            "usage_patterns": {
                "time_of_day": dict(USAGE_PATTERNS["time_of_day"]),
                "day_of_week": dict(USAGE_PATTERNS["day_of_week"])
            }
        }
        
        try:
            with open(filename, 'w') as f:
                json.dump(data, f, indent=2)
                
            logger.info(f"Analytics data saved to {filename}")
            return True
        except Exception as e:
            logger.error(f"Failed to save analytics data: {e}")
            return False
            
    async def load_analytics_data(self, filename: str = "analytics_data.json"):
        """Load analytics data from file
        
        Args:
            filename: File to load data from
            
        Returns:
            bool: Success
        """
        try:
            if os is None.path.exists(filename):
                logger.warning(f"Analytics data file {filename} does not exist")
                return False
                
            with open(filename, 'r') as f:
                data = json.load(f)
                
            # Update analytics data
            ANALYTICS_DATA["command_usage"] = defaultdict(int, data.get("command_usage", {}))
            ANALYTICS_DATA["feature_usage"] = defaultdict(int, data.get("feature_usage", {}))
            ANALYTICS_DATA["error_frequency"] = defaultdict(int, data.get("error_frequency", {}))
            
            # Update resource usage and performance trends
            if "resource_usage" in data:
                ANALYTICS_DATA["resource_usage"] = data["resource_usage"]
                
            if "performance_trends" in data:
                ANALYTICS_DATA["performance_trends"] = data["performance_trends"]
                
            # Update predictions
            if "predictions" in data:
                predictions = data["predictions"]
                
                if "resource_forecast" in predictions:
                    PREDICTIONS["resource_forecast"] = predictions["resource_forecast"]
                    
                if "error_likelihood" in predictions:
                    PREDICTIONS["error_likelihood"] = predictions["error_likelihood"]
                    
                if "feature_adoption" in predictions:
                    PREDICTIONS["feature_adoption"] = predictions["feature_adoption"]
                    
            # Update usage patterns
            if "usage_patterns" in data:
                usage_patterns = data["usage_patterns"]
                
                if "time_of_day" in usage_patterns:
                    USAGE_PATTERNS["time_of_day"] = defaultdict(int, usage_patterns["time_of_day"])
                    
                if "day_of_week" in usage_patterns:
                    USAGE_PATTERNS["day_of_week"] = defaultdict(int, usage_patterns["day_of_week"])
                    
            logger.info(f"Analytics data loaded from {filename}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load analytics data: {e}")
            return False
            
    def format_insights_embed(self, insights: Dict[str, Any]) -> Dict[str, Any]:
        """Format analytics insights for Discord embed
        
        Args:
            insights: Analytics insights
            
        Returns:
            Embed data
        """
        embed = {
            "title": "Bot Analytics Insights",
            "color": 0x3498DB,  # Blue
            "timestamp": datetime.utcnow().isoformat(),
            "fields": []
        }
        
        # Add usage stats
        usage_stats = "**Popular Commands:**\n"
        for cmd, count in insights["popular_commands"]:
            usage_stats += f"• `{cmd}`: {count} uses\n"
            
        embed["fields"].append({
            "name": "Usage Statistics",
            "value": usage_stats,
            "inline": False
        })
        
        # Add peak usage times
        peak_times = "**Peak Hours:**\n"
        for hour, count in insights["peak_usage"]["hours"]:
            # Convert hour to 12-hour format
            hour_12 = f"{hour % 12 or 12} {'AM' if hour < 12 else 'PM'}"
            peak_times += f"• {hour_12}: {count} commands\n"
            
        peak_times += "\n**Busiest Days:**\n"
        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        for day_num, count in insights["peak_usage"]["days"]:
            peak_times += f"• {days[day_num]}: {count} commands\n"
            
        embed["fields"].append({
            "name": "Activity Patterns",
            "value": peak_times,
            "inline": False
        })
        
        # Add error insights
        error_insights = "**Error-Prone Commands:**\n"
        for cmd, rate in insights["error_prone_commands"]:
            error_insights += f"• `{cmd}`: {rate:.1%} error rate\n"
            
        error_insights += "\n**Commands at Risk:**\n"
        for cmd, risk in insights["error_risks"]:
            risk_level = "High" if risk > 0.3 else "Medium" if risk > 0.1 else "Low"
            error_insights += f"• `{cmd}`: {risk_level} risk\n"
            
        embed["fields"].append({
            "name": "Error Insights",
            "value": error_insights,
            "inline": False
        })
        
        # Add active users/guilds
        activity = f"**Guilds:** {insights['active_guilds']['active_24h']}/{insights['active_guilds']['total']} active in 24h\n"
        activity += f"**Users:** {insights['active_users']['active_24h']}/{insights['active_users']['total']} active in 24h\n"
        
        embed["fields"].append({
            "name": "Active Users & Guilds",
            "value": activity,
            "inline": False
        })
        
        # Add resource forecast
        if insights["resource_forecast"]:
            forecast = "**7-Day Forecast:**\n"
            for day in insights["resource_forecast"]:
                day_num = day["day"]
                mem_change = (day["memory_projected"] / insights["resource_forecast"][0]["memory_projected"] - 1) * 100
                forecast += f"• Day {day_num}: Memory {mem_change:+.1f}%\n"
                
            embed["fields"].append({
                "name": "Resource Forecast",
                "value": forecast,
                "inline": False
            })
            
        return embed


async def setup_analytics(bot):
    """Set up analytics system
    
    Args:
        bot: Discord bot instance
        
    Returns:
        AnalyticsManager: Analytics manager
    """
    analytics = AnalyticsManager(bot)
    bot.analytics = analytics
    
    # Load previous analytics data if available is not None
    await analytics.load_analytics_data()
    
    # Set up background task
    analytics_task = asyncio.create_task(run_analytics_loop(analytics))
    
    logger.info("Analytics system initialized")
    
    return analytics
    

async def run_analytics_loop(analytics: AnalyticsManager, interval: int = 3600):
    """Run analytics in a background loop
    
    Args:
        analytics: Analytics manager
        interval: Interval in seconds
    """
    try:
        while True:
            await asyncio.sleep(interval)
            await analytics.run_analytics()
            
            # Save analytics data
            await analytics.save_analytics_data()
            
    except asyncio.CancelledError:
        logger.info("Analytics loop cancelled")
    except Exception as e:
        logger.error(f"Error in analytics loop: {e}")
        logger.error(traceback.format_exc())