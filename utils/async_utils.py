"""
Asynchronous Utilities for the Tower of Temptation PvP Statistics Discord Bot.

This module provides:
1. Asynchronous caching
2. Rate limiting
3. Retry mechanisms
4. Semaphore-based concurrency control
"""
import asyncio
import inspect
import logging
import time
import functools
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Set, Tuple, TypeVar, Callable, Coroutine, Type

logger = logging.getLogger(__name__)

# Type variables for generics
T = TypeVar('T')
R = TypeVar('R')

class AsyncCache:
    """Asynchronous cache for expensive function calls"""
    
    # Global cache storage
    _cache: Dict[str, Dict[Tuple, Tuple[Any, datetime]]] = {}
    
    def __init__(self, ttl: int = 300):
        """Initialize a cache instance with specified TTL
        
        Args:
            ttl: Time-to-live in seconds (default: 300)
        """
        self.ttl = ttl
        # Instance-specific cache tracking
        self.cache_key = f"instance_{id(self)}"
        if self.cache_key not in self.__class__._cache:
            self.__class__._cache[self.cache_key] = {}
            
    async def get(self, key: str) -> Any:
        """Get a value from the cache
        
        Args:
            key: Cache key
            
        Returns:
            Any: Cached value or None if found is None or expired
        """
        if self.cache_key not in self.__class__._cache:
            return None
            
        # Convert to tuple key format
        tuple_key = (key,)
        if tuple_key not in self.__class__._cache[self.cache_key]:
            return None
            
        result, timestamp = self.__class__._cache[self.cache_key][tuple_key]
        if datetime.utcnow() - timestamp < timedelta(seconds=self.ttl):
            return result
            
        # Expired
        return None
        
    async def set(self, key: str, value: Any) -> None:
        """Set a value in the cache
        
        Args:
            key: Cache key
            value: Value to cache
        """
        if self.cache_key not in self.__class__._cache:
            self.__class__._cache[self.cache_key] = {}
            
        # Convert string key to a single-element tuple to maintain type compatibility
        tuple_key = (key,)
        self.__class__._cache[self.cache_key][tuple_key] = (value, datetime.utcnow())
    
    @classmethod
    def cached(cls, ttl: int = 300):
        """Decorator for caching async function results
        
        Args:
            ttl: Time to live in seconds (default: 300)
            
        Returns:
            Callable: Decorated function
        """
        def decorator(func):
            # Initialize cache for this function
            func_name = func.__qualname__
            if func_name not in cls._cache:
                cls._cache[func_name] = {}
                
            @functools.wraps(func)
            async def wrapper(*args, **kwargs):
                # Create cache key from arguments
                cache_key = cls._create_cache_key(args, kwargs)
                
                # Check cache
                if cache_key in cls._cache[func_name]:
                    result, timestamp = cls._cache[func_name][cache_key]
                    if datetime.utcnow() - timestamp < timedelta(seconds=ttl):
                        # Cache hit
                        return result
                
                # Cache miss or expired, call function
                result = await func(*args, **kwargs)
                
                # Store result in cache
                cls._cache[func_name][cache_key] = (result, datetime.utcnow())
                
                return result
                
            return wrapper
        return decorator
    
    @classmethod
    def _create_cache_key(cls, args: Tuple, kwargs: Dict) -> Tuple:
        """Create cache key from function arguments
        
        Args:
            args: Positional arguments
            kwargs: Keyword arguments
            
        Returns:
            Tuple: Cache key
        """
        # Convert args to hashable
        hashable_args = tuple(
            tuple(arg) if isinstance(arg, list) else 
            frozenset(arg.items()) if isinstance(arg, dict) else 
            arg
            for arg in args
        )
        
        # Convert kwargs to hashable
        hashable_kwargs = tuple(sorted(
            (k, tuple(v) if isinstance(v, list) else 
             frozenset(v.items()) if isinstance(v, dict) else 
             v)
            for k, v in kwargs.items()
        ))
        
        return hashable_args + hashable_kwargs
    
    @classmethod
    def invalidate(cls, func: Callable, *args, **kwargs) -> bool:
        """Invalidate cache entry for function with given arguments
        
        Args:
            func: Function to invalidate cache for
            *args: Function arguments
            **kwargs: Function keyword arguments
            
        Returns:
            bool: True if cache is not None was invalidated
        """
        func_name = func.__qualname__
        if func_name not in cls._cache:
            return False
            
        if args is None and not kwargs:
            # Invalidate all entries for function
            cls._cache[func_name] = {}
            return True
            
        # Invalidate specific entry
        cache_key = cls._create_cache_key(args, kwargs)
        if cache_key in cls._cache[func_name]:
            del cls._cache[func_name][cache_key]
            return True
            
        return False
    
    @classmethod
    def invalidate_pattern(cls, func: Callable, pattern_args: List[Any]) -> int:
        """Invalidate cache entries matching a pattern
        
        Args:
            func: Function to invalidate cache for
            pattern_args: List of argument patterns to match
            
        Returns:
            int: Number of invalidated entries
        """
        func_name = func.__qualname__
        if func_name not in cls._cache:
            return 0
            
        # Find matching keys
        invalidated = 0
        to_delete = []
        
        for cache_key in cls._cache[func_name]:
            # Check if key is not None args match pattern
            if len(cache_key) >= len(pattern_args):
                match = True
                for i, pattern in enumerate(pattern_args):
                    if pattern is not None and cache_key[i] != pattern:
                        match = False
                        break
                        
                if match:
                    to_delete.append(cache_key)
                    invalidated += 1
        
        # Delete matched keys
        for key in to_delete:
            del cls._cache[func_name][key]
            
        return invalidated
    
    @classmethod
    def clear(cls) -> None:
        """Clear entire cache"""
        cls._cache = {}
    
    @classmethod
    def get_stats(cls) -> Dict[str, Dict[str, int]]:
        """Get cache statistics
        
        Returns:
            Dict: Cache statistics
        """
        stats = {}
        for func_name, cache in cls._cache.items():
            # Calculate age of entries
            now = datetime.utcnow()
            ages = [int((now - timestamp).total_seconds()) for _, timestamp in cache.values()]
            
            if not ages:
                continue
                
            stats[func_name] = {
                "count": len(cache),
                "min_age": min(ages) if ages else 0,
                "max_age": max(ages) if ages else 0,
                "avg_age": sum(ages) / len(ages) if ages else 0
            }
            
        return stats

class RateLimiter:
    """Rate limiter for API calls"""
    
    def __init__(self, calls: int, period: float, spread: bool = True):
        """Initialize rate limiter
        
        Args:
            calls: Maximum number of calls per period
            period: Time period in seconds
            spread: Spread calls evenly across period (default: True)
        """
        self.calls = calls
        self.period = period
        self.spread = spread
        self.timestamps: List[float] = []
        self.lock = asyncio.Lock()
    
    async def acquire(self) -> None:
        """Acquire permission to proceed
        
        This method will block until rate limit allows execution
        """
        async with self.lock:
            now = time.time()
            
            # Remove timestamps older than period
            self.timestamps = [ts for ts in self.timestamps if now - ts < self.period]
            
            if len(self.timestamps) >= self.calls:
                # Rate limit exceeded, wait for next slot
                oldest = min(self.timestamps)
                wait_time = self.period - (now - oldest)
                
                if self.spread and self.timestamps:
                    # Spread calls evenly
                    wait_time = max(wait_time, self.period / self.calls)
                    
                if wait_time > 0:
                    logger.debug(f"Rate limit exceeded, waiting {wait_time:.2f}s")
                    # Release lock while waiting
                    self.lock.release()
                    await asyncio.sleep(wait_time)
                    await self.lock.acquire()
                    
                    # Recalculate after waiting
                    now = time.time()
                    self.timestamps = [ts for ts in self.timestamps if now - ts < self.period]
            
            # Add current timestamp
            self.timestamps.append(now)

def retryable(max_retries: int = 3, delay: float = 2.0, backoff: float = 1.5, 
              exceptions: Any = Exception):
    """Decorator for retrying failed async functions
    
    Args:
        max_retries: Maximum number of retries (default: 3)
        delay: Initial delay between retries in seconds (default: 2.0)
        backoff: Backoff multiplier (default: 1.5)
        exceptions: Exception type(s) to retry on (default: Exception)
        
    Returns:
        Callable: Decorated function
    """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            retries = 0
            current_delay = delay
            
            while True:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    # Support for multiple exception types or single exception
                    should_retry = False
                    
                    # Check if we should retry this exception
                    if isinstance(exceptions, (list, tuple)):
                        # Multiple exception types
                        for exc_type in exceptions:
                            if isinstance(e, exc_type):
                                should_retry = True
                                break
                    else:
                        # Single exception type
                        should_retry = isinstance(e, exceptions)
                        
                    if not should_retry:
                        raise
                    retries += 1
                    if retries > max_retries:
                        # Max retries exceeded, re-raise exception
                        logger.error(f"Max retries ({max_retries}) exceeded for {func.__name__}: {str(e)}")
                        raise
                        
                    # Add jitter to delay (±20%)
                    jitter = random.uniform(0.8, 1.2)
                    wait_time = current_delay * jitter
                    
                    logger.warning(
                        f"Retry {retries}/{max_retries} for {func.__name__} in {wait_time:.2f}s: {str(e)}"
                    )
                    
                    # Wait before retrying
                    await asyncio.sleep(wait_time)
                    
                    # Increase delay for next retry
                    current_delay *= backoff
                    
        return wrapper
    return decorator

async def semaphore_gather(semaphore: asyncio.Semaphore, coros: List[Coroutine]) -> List[Any]:
    """Run coroutines with semaphore for concurrency control
    
    Args:
        semaphore: Semaphore for limiting concurrency
        coros: List of coroutines to run
        
    Returns:
        List: Results of coroutines or exceptions if return_exceptions=True
    """
    async def _run_with_semaphore(coro):
        async with semaphore:
            return await coro
            
    return await asyncio.gather(*[_run_with_semaphore(coro) for coro in coros], 
                               return_exceptions=True)


class BackgroundTask:
    """Class for managing background tasks with proper lifecycle management"""
    
    def __init__(self, coro_func, minutes: float = 5.0, name: Optional[str] = None,
                 max_consecutive_errors: int = 3, initial_delay: float = 5.0):
        """Initialize background task
        
        Args:
            coro_func: Coroutine function to run as a task
            minutes: How often to run the task in minutes (default: 5.0)
            name: Name for the task (default: function name)
            max_consecutive_errors: Maximum number of consecutive errors before stopping (default: 3)
            initial_delay: Initial delay before first run in seconds (default: 5.0)
        """
        self.coro_func = coro_func
        self.minutes = minutes
        self.name = name or coro_func.__name__
        self.max_consecutive_errors = max_consecutive_errors
        self.initial_delay = initial_delay
        
        self.task = None
        self.is_running = False
        self.error_count = 0
        self.last_error = None
        self.last_success = None
        self.total_runs = 0
        self.successful_runs = 0
        
        # Create logger
        self.logger = logging.getLogger(f"background_task.{self.name}")
    
    async def _task_wrapper(self):
        """Wrapper around the actual task function"""
        # Wait initial delay
        await asyncio.sleep(self.initial_delay)
        
        self.logger.info(f"Background task '{self.name}' started")
        
        while self.is_running:
            start_time = time.time()
            
            try:
                # Run the task
                await self.coro_func()
                
                # Update success metrics
                self.error_count = 0
                self.last_success = datetime.utcnow()
                self.successful_runs += 1
                
            except asyncio.CancelledError:
                # Task was cancelled, exit cleanly
                self.logger.info(f"Background task '{self.name}' cancelled")
                break
                
            except Exception as e:
                # Log error and update metrics
                self.error_count += 1
                self.last_error = (datetime.utcnow(), str(e))
                
                self.logger.error(f"Error in background task '{self.name}': {str(e)}", exc_info=True)
                
                if self.error_count >= self.max_consecutive_errors:
                    self.logger.critical(
                        f"Background task '{self.name}' stopped after {self.error_count} consecutive errors"
                    )
                    self.is_running = False
                    break
            
            finally:
                # Update total runs
                self.total_runs += 1
                
                # Calculate time taken and sleep accordingly
                elapsed = time.time() - start_time
                sleep_time = max(0, (self.minutes * 60) - elapsed)
                
                if sleep_time > 0 and self.is_running:
                    await asyncio.sleep(sleep_time)
        
        self.logger.info(f"Background task '{self.name}' stopped")
    
    def start(self):
        """Start the background task"""
        if self.is_running:
            raise RuntimeError(f"Background task '{self.name}' already running")
            
        self.is_running = True
        self.task = asyncio.create_task(self._task_wrapper())
        
        return self.task
    
    def stop(self):
        """Stop the background task"""
        if not self.is_running:
            return
            
        self.is_running = False
        if self.task and not self.task.done():
            self.task.cancel()
    
    def restart(self):
        """Restart the background task"""
        self.stop()
        self.error_count = 0
        return self.start()
    
    def get_status(self) -> Dict[str, Any]:
        """Get task status
        
        Returns:
            Dict: Status information
        """
        return {
            "name": self.name,
            "running": self.is_running,
            "total_runs": self.total_runs,
            "successful_runs": self.successful_runs,
            "error_count": self.error_count,
            "last_success": self.last_success,
            "last_error": self.last_error,
            "interval_minutes": self.minutes
        }