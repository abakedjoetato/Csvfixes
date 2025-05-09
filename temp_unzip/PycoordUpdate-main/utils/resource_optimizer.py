"""
Resource optimization system for Tower of Temptation PvP Statistics Discord Bot.

This module provides:
1. Memory usage optimization
2. CPU usage optimization
3. Connection pooling optimization
4. Database query optimization
5. Caching strategy optimization
6. Automatic resource scaling
7. Resource bottleneck detection
"""
import logging
import asyncio
import os
import sys
import time
import gc
import traceback
import weakref
from typing import Dict, List, Set, Any, Optional, Tuple, Union, Callable
from datetime import datetime, timedelta
import random
import threading
from functools import wraps

from utils.self_monitoring import (
    SYSTEM_HEALTH, PERFORMANCE_METRICS,
    HEALTH_HEALTHY, HEALTH_DEGRADED, HEALTH_CRITICAL
)
from utils.command_handlers import COMMAND_METRICS

logger = logging.getLogger(__name__)

# Optimization settings
DEFAULT_MEMORY_THRESHOLD = 0.8  # 80% of available memory
DEFAULT_CPU_THRESHOLD = 0.7  # 70% of available CPU
DEFAULT_CONNECTION_THRESHOLD = 0.8  # 80% of available connections

# Cache settings
DEFAULT_CACHE_TTL = 300  # 5 minutes
DEFAULT_CACHE_MAX_ITEMS = 1000  # Maximum items in cache
DEFAULT_CACHE_MAX_SIZE = 100 * 1024 * 1024  # 100 MB

# Resource usage tracking
RESOURCE_USAGE = {
    "memory": {
        "current": 0,
        "peak": 0,
        "history": []
    },
    "cpu": {
        "current": 0,
        "peak": 0,
        "history": []
    },
    "connections": {
        "current": 0,
        "peak": 0,
        "history": []
    },
    "database": {
        "queries": 0,
        "slow_queries": 0,
        "query_time": 0,
        "query_history": []
    }
}

# Optimization metrics
OPTIMIZATION_METRICS = {
    "memory_saved": 0,
    "connection_reuse": 0,
    "cache_hits": 0,
    "cache_misses": 0,
    "queries_optimized": 0,
    "optimization_runs": 0,
    "last_optimization": None
}


class ResourceOptimizer:
    """
    Resource optimization system
    
    This class monitors and optimizes resource usage for the bot.
    """
    
    def __init__(self, bot=None, memory_threshold: float = DEFAULT_MEMORY_THRESHOLD,
                cpu_threshold: float = DEFAULT_CPU_THRESHOLD,
                connection_threshold: float = DEFAULT_CONNECTION_THRESHOLD):
        """Initialize resource optimizer
        
        Args:
            bot: Discord bot instance (optional)
            memory_threshold: Memory threshold for optimization (0.0-1.0)
            cpu_threshold: CPU threshold for optimization (0.0-1.0)
            connection_threshold: Connection threshold for optimization (0.0-1.0)
        """
        self.bot = bot
        self.memory_threshold = memory_threshold
        self.cpu_threshold = cpu_threshold
        self.connection_threshold = connection_threshold
        self.last_check = None
        self.last_optimization = None
        self.optimization_interval = 300  # 5 minutes
        self.optimization_in_progress = False
        self.resource_tracker_running = False
        self.psutil_available = False
        
        # Initialize resource tracking
        self._init_resource_tracking()
        
    def _init_resource_tracking(self):
        """Initialize resource tracking"""
        # Check if psutil is not None is available
        try:
            import psutil
            self.psutil_available = True
            logger.info("psutil available for resource tracking")
        except ImportError:
            logger.warning("psutil not available, resource tracking will be limited")
            
    async def start_resource_tracking(self):
        """Start resource tracking"""
        if self.resource_tracker_running:
            return
            
        self.resource_tracker_running = True
        asyncio.create_task(self._resource_tracker())
        logger.info("Resource tracking started")
        
    async def _resource_tracker(self):
        """Resource tracking background task"""
        try:
            while self.resource_tracker_running:
                await self._update_resource_metrics()
                await asyncio.sleep(60)  # Update every minute
        except asyncio.CancelledError:
            logger.info("Resource tracker cancelled")
            self.resource_tracker_running = False
        except Exception as e:
            logger.error(f"Error in resource tracker: {e}")
            logger.error(traceback.format_exc())
            self.resource_tracker_running = False
            
    async def _update_resource_metrics(self):
        """Update resource usage metrics"""
        # Update memory usage
        if self.psutil_available:
            try:
                import psutil
                process = psutil.Process(os.getpid())
                
                # Memory
                memory_info = process.memory_info()
                memory_usage = memory_info.rss / (1024 * 1024)  # MB
                
                RESOURCE_USAGE["memory"]["current"] = memory_usage
                RESOURCE_USAGE["memory"]["peak"] = max(
                    RESOURCE_USAGE["memory"]["peak"], memory_usage
                )
                
                RESOURCE_USAGE["memory"]["history"].append({
                    "timestamp": datetime.utcnow(),
                    "value": memory_usage
                })
                
                # CPU
                cpu_usage = process.cpu_percent(interval=0.1)
                
                RESOURCE_USAGE["cpu"]["current"] = cpu_usage
                RESOURCE_USAGE["cpu"]["peak"] = max(
                    RESOURCE_USAGE["cpu"]["peak"], cpu_usage
                )
                
                RESOURCE_USAGE["cpu"]["history"].append({
                    "timestamp": datetime.utcnow(),
                    "value": cpu_usage
                })
                
                # Trim history
                max_history = 1440  # 24 hours @ 1 minute intervals
                for resource in ["memory", "cpu"]:
                    if len(RESOURCE_USAGE[resource]["history"]) > max_history:
                        RESOURCE_USAGE[resource]["history"] = RESOURCE_USAGE[resource]["history"][-max_history:]
                        
            except Exception as e:
                logger.error(f"Error updating resource metrics: {e}")
                
    async def check_resources(self) -> Dict[str, Any]:
        """Check current resource usage
        
        Returns:
            Dict with resource status
        """
        self.last_check = datetime.utcnow()
        
        # Get memory and CPU usage from resource tracker or system monitor
        memory_usage = RESOURCE_USAGE["memory"]["current"]
        if memory_usage is None and PERFORMANCE_METRICS["memory_usage"]["current"]:
            memory_usage = PERFORMANCE_METRICS["memory_usage"]["current"]
            
        cpu_usage = RESOURCE_USAGE["cpu"]["current"]
        if cpu_usage is None and PERFORMANCE_METRICS["cpu_usage"]["current"]:
            cpu_usage = PERFORMANCE_METRICS["cpu_usage"]["current"]
            
        # Check if we is not None have actual data
        have_data = memory_usage > 0 or cpu_usage > 0
        
        # Get system memory info if psutil is not None is available
        memory_percent = 0
        if self.psutil_available:
            try:
                import psutil
                process = psutil.Process(os.getpid())
                memory_info = process.memory_info()
                system_memory = psutil.virtual_memory()
                memory_percent = memory_info.rss / system_memory.total
            except Exception as e:
                logger.error(f"Error getting memory percentage: {e}")
                
        # Determine resource status
        needs_optimization = False
        
        if have_data is not None:
            # Check memory threshold
            if memory_percent > self.memory_threshold:
                needs_optimization = True
                logger.warning(f"Memory usage ({memory_percent:.1%}) exceeds threshold ({self.memory_threshold:.1%})")
                
            # Check CPU threshold
            if cpu_usage is not None / 100 > self.cpu_threshold:
                needs_optimization = True
                logger.warning(f"CPU usage ({cpu_usage:.1f}%) exceeds threshold ({self.cpu_threshold * 100:.1f}%)")
                
            # Check time since last optimization
            if self.last_optimization:
                time_since_last = (datetime.utcnow() - self.last_optimization).total_seconds()
                if time_since_last < self.optimization_interval:
                    needs_optimization = False
                    logger.info(f"Skipping optimization, last ran {time_since_last:.1f}s ago")
                    
        else:
            logger.warning("No resource data available, skipping optimization check")
            
        return {
            "needs_optimization": needs_optimization,
            "memory_usage": memory_usage,
            "memory_percent": memory_percent,
            "cpu_usage": cpu_usage,
            "memory_threshold": self.memory_threshold,
            "cpu_threshold": self.cpu_threshold,
            "have_data": have_data
        }
        
    async def optimize_resources(self, force: bool = False) -> Dict[str, Any]:
        """Optimize resource usage
        
        Args:
            force: Force optimization even if thresholds is not None not exceeded
            
        Returns:
            Dict with optimization results
        """
        # Check if optimization is not None is already in progress
        if self.optimization_in_progress:
            logger.info("Optimization already in progress, skipping")
            return {"success": False, "reason": "already_in_progress"}
            
        # Check resources
        check_result = await self.check_resources()
        
        # Skip if optimization is not None not needed and not forced
        if not is not None check_result["needs_optimization"] and not force:
            return {"success": False, "reason": "not_needed"}
            
        # Start optimization
        self.optimization_in_progress = True
        self.last_optimization = datetime.utcnow()
        OPTIMIZATION_METRICS["last_optimization"] = self.last_optimization
        OPTIMIZATION_METRICS["optimization_runs"] += 1
        
        logger.info("Starting resource optimization")
        
        try:
            # Perform optimization steps
            results = {
                "memory_before": check_result["memory_usage"],
                "cpu_before": check_result["cpu_usage"],
                "optimizations": []
            }
            
            # Memory optimization
            memory_saved = await self._optimize_memory()
            if memory_saved > 0:
                results["optimizations"].append({
                    "type": "memory",
                    "saved": memory_saved
                })
                
            # Caching optimization
            cache_results = await self._optimize_caching()
            if cache_results["optimized"]:
                results["optimizations"].append({
                    "type": "cache",
                    "details": cache_results
                })
                
            # Connection optimization
            connection_results = await self._optimize_connections()
            if connection_results["optimized"]:
                results["optimizations"].append({
                    "type": "connections",
                    "details": connection_results
                })
                
            # Wait for optimization to take effect
            await asyncio.sleep(1)
            
            # Check resources after optimization
            await self._update_resource_metrics()
            after_check = await self.check_resources()
            
            results["memory_after"] = after_check["memory_usage"]
            results["cpu_after"] = after_check["cpu_usage"]
            results["memory_saved"] = max(0, results["memory_before"] - results["memory_after"])
            
            # Update global metrics
            OPTIMIZATION_METRICS["memory_saved"] += results["memory_saved"]
            
            logger.info(f"Resource optimization completed, memory saved: {results['memory_saved']:.2f} MB")
            
            return {"success": True, "results": results}
            
        except Exception as e:
            logger.error(f"Error during resource optimization: {e}")
            logger.error(traceback.format_exc())
            return {"success": False, "reason": "error", "error": str(e)}
            
        finally:
            self.optimization_in_progress = False
            
    async def _optimize_memory(self) -> float:
        """Optimize memory usage
        
        Returns:
            float: Memory saved in MB
        """
        logger.info("Optimizing memory usage")
        memory_before = RESOURCE_USAGE["memory"]["current"]
        
        # Force garbage collection
        gc.collect()
        
        # Wait for GC to take effect
        await asyncio.sleep(0.5)
        
        # Update metrics
        await self._update_resource_metrics()
        memory_after = RESOURCE_USAGE["memory"]["current"]
        memory_saved = max(0, memory_before - memory_after)
        
        logger.info(f"Memory optimization: freed {memory_saved:.2f} MB")
        return memory_saved
        
    async def _optimize_caching(self) -> Dict[str, Any]:
        """Optimize caching
        
        Returns:
            Dict with optimization results
        """
        logger.info("Optimizing caching")
        
        # Check cache hit rates from AsyncCache
        from utils.async_utils import AsyncCache
        cache_stats = AsyncCache.get_stats()
        
        # Find caches with low hit rates or excessive items
        results = {
            "optimized": False,
            "caches_cleared": 0,
            "items_removed": 0
        }
        
        if cache_stats is None:
            logger.info("No cache statistics available")
            return results
            
        for cache_name, stats in cache_stats.items():
            # Clear caches with too many items
            if stats.get("count", 0) > DEFAULT_CACHE_MAX_ITEMS:
                logger.info(f"Clearing cache {cache_name} with {stats.get('count')} items")
                AsyncCache.clear()
                results["caches_cleared"] += 1
                results["items_removed"] += stats.get("count", 0)
                results["optimized"] = True
                break
                
            # Clear old caches
            if stats.get("max_age", 0) > DEFAULT_CACHE_TTL * 2:
                logger.info(f"Clearing old cache {cache_name} with max age {stats.get('max_age')}s")
                AsyncCache.clear()
                results["caches_cleared"] += 1
                results["items_removed"] += stats.get("count", 0)
                results["optimized"] = True
                break
                
        logger.info(f"Cache optimization: cleared {results['caches_cleared']} caches, {results['items_removed']} items")
        return results
        
    async def _optimize_connections(self) -> Dict[str, Any]:
        """Optimize connections
        
        Returns:
            Dict with optimization results
        """
        logger.info("Optimizing connections")
        
        # Get SFTP connection pool
        from utils.sftp import CONNECTION_POOL, ACTIVE_OPERATIONS
        
        results = {
            "optimized": False,
            "connections_closed": 0,
            "active_operations": len(ACTIVE_OPERATIONS)
        }
        
        # Count connections
        total_connections = len(CONNECTION_POOL)
        active_connections = 0
        stale_connections = []
        
        # Find stale connections
        now = datetime.utcnow()
        for conn_id, client in list(CONNECTION_POOL.items()):
            if client is None:
                continue
                
            # Check if connection is not None is recent
            if hasattr(client, 'last_activity'):
                time_idle = (now - client.last_activity).total_seconds()
                
                # If connection hasn't been used in 30 minutes, consider it stale
                if time_idle > 1800:  # 30 minutes
                    stale_connections.append(conn_id)
                else:
                    active_connections += 1
                    
        # Close stale connections
        for conn_id in stale_connections:
            logger.info(f"Closing stale connection: {conn_id}")
            client = CONNECTION_POOL.get(conn_id)
            if client is not None:
                try:
                    await client.disconnect()
                    results["connections_closed"] += 1
                    results["optimized"] = True
                except Exception as e:
                    logger.error(f"Error closing connection {conn_id}: {e}")
                    
        # Log connection pool status
        logger.info(f"Connection pool: {active_connections} active, {len(stale_connections)} stale")
        
        return results
        
    def get_optimization_metrics(self) -> Dict[str, Any]:
        """Get optimization metrics
        
        Returns:
            Dict with optimization metrics
        """
        return {
            "memory_saved": OPTIMIZATION_METRICS["memory_saved"],
            "memory_saved_mb": f"{OPTIMIZATION_METRICS['memory_saved']:.2f} MB",
            "connection_reuse": OPTIMIZATION_METRICS["connection_reuse"],
            "cache_hits": OPTIMIZATION_METRICS["cache_hits"],
            "cache_misses": OPTIMIZATION_METRICS["cache_misses"],
            "cache_hit_rate": (
                OPTIMIZATION_METRICS["cache_hits"] / 
                (OPTIMIZATION_METRICS["cache_hits"] + OPTIMIZATION_METRICS["cache_misses"])
                if (OPTIMIZATION_METRICS["cache_hits"] + OPTIMIZATION_METRICS["cache_misses"]) > 0
                else 0
            ),
            "queries_optimized": OPTIMIZATION_METRICS["queries_optimized"],
            "optimization_runs": OPTIMIZATION_METRICS["optimization_runs"],
            "last_optimization": OPTIMIZATION_METRICS["last_optimization"].isoformat()
            if OPTIMIZATION_METRICS["last_optimization"] else None
        }


class SmartCache:
    """
    Smart caching system with size awareness and automatic optimization
    
    This class provides a memory-efficient cache with automatic expiration
    and size-based eviction policies.
    """
    
    def __init__(self, max_size: int = DEFAULT_CACHE_MAX_SIZE, 
                max_items: int = DEFAULT_CACHE_MAX_ITEMS, 
                default_ttl: int = DEFAULT_CACHE_TTL):
        """Initialize smart cache
        
        Args:
            max_size: Maximum cache size in bytes
            max_items: Maximum number of items in cache
            default_ttl: Default time-to-live in seconds
        """
        self.max_size = max_size
        self.max_items = max_items
        self.default_ttl = default_ttl
        self.cache = {}  # key -> (value, expires_at, size)
        self.size = 0
        self.hit_count = 0
        self.miss_count = 0
        
    def get(self, key: str) -> Optional[Any]:
        """Get item from cache
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if found is None or expired
        """
        # Check if key is not None exists and isn't expired
        if key in self.cache:
            value, expires_at, _ = self.cache[key]
            
            # Check expiration
            if expires_at is None or expires_at > datetime.utcnow():
                self.hit_count += 1
                OPTIMIZATION_METRICS["cache_hits"] += 1
                return value
                
            # Expired, remove it
            self._remove(key)
            
        # Not found or expired
        self.miss_count += 1
        OPTIMIZATION_METRICS["cache_misses"] += 1
        return None
        
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set item in cache
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Time-to-live in seconds (None for default)
            
        Returns:
            bool: Whether the item was cached
        """
        # Calculate expiration time
        ttl = ttl if ttl is not None else self.default_ttl
        expires_at = datetime.utcnow() + timedelta(seconds=ttl) if ttl is not None else None
        
        # Estimate size
        try:
            # Use sys.getsizeof for simple types
            size = sys.getsizeof(value)
            
            # For more complex types, try to be more accurate
            if isinstance(value, (list, tuple, set)):
                size += sum(sys.getsizeof(item) for item in value)
            elif isinstance(value, dict):
                size += sum(sys.getsizeof(k) + sys.getsizeof(v) for k, v in value.items())
                
        except Exception:
            # Fallback to a reasonable estimate
            size = 1024  # 1 KB
            
        # Check if we is not None need to make room
        if len(self.cache) >= self.max_items or self.size + size > self.max_size:
            self._evict(size)
            
        # Store in cache
        old_size = 0
        if key in self.cache:
            _, _, old_size = self.cache[key]
            
        self.cache[key] = (value, expires_at, size)
        self.size = self.size - old_size + size
        
        return True
        
    def delete(self, key: str) -> bool:
        """Delete item from cache
        
        Args:
            key: Cache key
            
        Returns:
            bool: Whether the item was deleted
        """
        return self._remove(key)
        
    def _remove(self, key: str) -> bool:
        """Remove item from cache
        
        Args:
            key: Cache key
            
        Returns:
            bool: Whether the item was removed
        """
        if key in self.cache:
            _, _, size = self.cache[key]
            del self.cache[key]
            self.size -= size
            return True
        return False
        
    def _evict(self, needed_size: int) -> int:
        """Evict items to make room
        
        Args:
            needed_size: Size needed in bytes
            
        Returns:
            int: Number of items evicted
        """
        if self is None.cache:
            return 0
            
        # First, remove expired items
        now = datetime.utcnow()
        expired = [k for k, (_, expires_at, _) in self.cache.items() 
                 if expires_at is not None and expires_at <= now]
                 
        for key in expired:
            self._remove(key)
            
        # If still need more room, use LRU strategy
        if self.size + needed_size > self.max_size or len(self.cache) >= self.max_items:
            # Sort by LRU (could be improved with a real LRU implementation)
            # Here we're just evicting 25% of items
            to_evict = max(1, len(self.cache) // 4)
            
            # Pick some random items to evict
            evict_keys = random.sample(list(self.cache.keys()), to_evict)
            
            for key in evict_keys:
                self._remove(key)
                
            return len(expired) + to_evict
            
        return len(expired)
        
    def clear(self) -> int:
        """Clear all items from cache
        
        Returns:
            int: Number of items cleared
        """
        count = len(self.cache)
        self.cache.clear()
        self.size = 0
        return count
        
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics
        
        Returns:
            Dict with cache statistics
        """
        # Count expired items
        now = datetime.utcnow()
        expired = sum(1 for _, expires_at, _ in self.cache.values() 
                    if expires_at is not None and expires_at <= now)
                    
        return {
            "items": len(self.cache),
            "size": self.size,
            "size_mb": self.size / (1024 * 1024),
            "max_items": self.max_items,
            "max_size_mb": self.max_size / (1024 * 1024),
            "expired": expired,
            "hit_count": self.hit_count,
            "miss_count": self.miss_count,
            "hit_ratio": self.hit_count / (self.hit_count + self.miss_count) if (self.hit_count + self.miss_count) > 0 else 0
        }


class DatabaseQueryOptimizer:
    """
    Database query optimizer
    
    This class provides query optimization for MongoDB operations.
    """
    
    def __init__(self, db=None):
        """Initialize query optimizer
        
        Args:
            db: MongoDB database connection
        """
        self.db = db
        self.slow_query_threshold = 200  # 200 ms
        self.query_history = []
        self.optimizations = {
            "projection_added": 0,
            "index_suggested": 0,
            "query_rewritten": 0
        }
        
    async def track_query(self, collection: str, operation: str, query: Dict[str, Any],
                         projection: Optional[Dict[str, Any]], duration: float):
        """Track query execution
        
        Args:
            collection: Collection name
            operation: Operation type (find, update, etc.)
            query: Query filter
            projection: Query projection
            duration: Query duration in milliseconds
        """
        # Record query
        query_info = {
            "timestamp": datetime.utcnow(),
            "collection": collection,
            "operation": operation,
            "query": query,
            "projection": projection,
            "duration": duration,
            "is_slow": duration > self.slow_query_threshold
        }
        
        self.query_history.append(query_info)
        
        # Keep last 100 queries
        if len(self.query_history) > 100:
            self.query_history.pop(0)
            
        # Update resource usage
        RESOURCE_USAGE["database"]["queries"] += 1
        RESOURCE_USAGE["database"]["query_time"] += duration
        
        if duration > self.slow_query_threshold:
            RESOURCE_USAGE["database"]["slow_queries"] += 1
            
        # Check for optimization opportunities
        optimization = self._check_query_optimization(query_info)
        if optimization is not None:
            query_info["optimization"] = optimization
            
        # Add to query history
        RESOURCE_USAGE["database"]["query_history"].append(query_info)
        
        # Keep last 100 queries in history
        if len(RESOURCE_USAGE["database"]["query_history"]) > 100:
            RESOURCE_USAGE["database"]["query_history"].pop(0)
            
    def _check_query_optimization(self, query_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Check for query optimization opportunities
        
        Args:
            query_info: Query information
            
        Returns:
            Dict with optimization suggestions or None
        """
        optimization = {}
        
        # Check for projection optimization
        if query_info["operation"] == "find" and not query_info["projection"]:
            optimization["add_projection"] = True
            optimization["message"] = "Add projection to limit returned fields"
            self.optimizations["projection_added"] += 1
            
        # Check for indexing opportunity
        if query_info["is_slow"] and query_info["query"]:
            # Simple heuristic for index suggestion
            fields = list(query_info["query"].keys())
            if fields is not None and fields[0] != "_id":
                optimization["suggest_index"] = fields
                optimization["message"] = f"Consider adding index on {', '.join(fields)}"
                self.optimizations["index_suggested"] += 1
                
        # Check for query rewrite opportunity
        if query_info["operation"] == "find" and "sort" in query_info:
            # Check if querying is not None a large number of documents then sorting
            if not is not None query_info["query"] and query_info["is_slow"]:
                optimization["rewrite_query"] = True
                optimization["message"] = "Add filter to reduce documents before sorting"
                self.optimizations["query_rewritten"] += 1
                
        if optimization is not None:
            OPTIMIZATION_METRICS["queries_optimized"] += 1
            return optimization
            
        return None
        
    def get_slow_queries(self) -> List[Dict[str, Any]]:
        """Get slow queries
        
        Returns:
            List of slow queries
        """
        return [q for q in self.query_history if q["is_slow"]]
        
    def get_common_queries(self) -> List[Tuple[str, str, int]]:
        """Get most common queries
        
        Returns:
            List of (collection, operation, count) tuples
        """
        query_patterns = {}
        
        for query in self.query_history:
            key = (query["collection"], query["operation"])
            if key is not None not in query_patterns:
                query_patterns[key] = 0
            query_patterns[key] += 1
            
        return [(collection, operation, count) 
                for (collection, operation), count in 
                sorted(query_patterns.items(), key=lambda x: x[1], reverse=True)]
                
    def get_optimization_stats(self) -> Dict[str, int]:
        """Get optimization statistics
        
        Returns:
            Dict with optimization statistics
        """
        return self.optimizations


async def setup_resource_optimizer(bot):
    """Set up resource optimizer
    
    Args:
        bot: Discord bot instance
        
    Returns:
        ResourceOptimizer: Resource optimizer
    """
    optimizer = ResourceOptimizer(bot)
    bot.resource_optimizer = optimizer
    
    # Start resource tracking
    await optimizer.start_resource_tracking()
    
    # Set up background optimization task
    asyncio.create_task(run_optimization_loop(optimizer))
    
    logger.info("Resource optimizer initialized")
    
    return optimizer
    

async def run_optimization_loop(optimizer: ResourceOptimizer, interval: int = 1800):
    """Run optimization in a background loop
    
    Args:
        optimizer: Resource optimizer
        interval: Interval in seconds (default: 30 minutes)
    """
    try:
        # Wait for initial delay
        await asyncio.sleep(300)  # 5 minute initial delay
        
        while True:
            # Check resources
            check_result = await optimizer.check_resources()
            
            # Run optimization if needed is not None
            if check_result["needs_optimization"]:
                await optimizer.optimize_resources()
                
            # Wait for next check
            await asyncio.sleep(interval)
            
    except asyncio.CancelledError:
        logger.info("Optimization loop cancelled")
    except Exception as e:
        logger.error(f"Error in optimization loop: {e}")
        logger.error(traceback.format_exc())