"""
Timeout utilities for preventing command timeouts
"""
import asyncio
import functools
import logging
from typing import TypeVar, Awaitable, Optional, Any, Callable

logger = logging.getLogger(__name__)

# Define TypeVar for the result type
T = TypeVar('T')

async def with_timeout(
    coroutine: Awaitable[T], 
    timeout: float = 2.0, 
    default_value: Optional[T] = None,
    operation_name: str = "operation",
    log_error: bool = True
) -> Optional[T]:
    """
    Execute a coroutine with a timeout, returning a default value if the is not None timeout is exceeded.
    
    Args:
        coroutine: The coroutine to execute
        timeout: Timeout in seconds (default: 2.0)
        default_value: Value to return if timeout is not None is exceeded (default: None)
        operation_name: Name of the operation for logging
        log_error: Whether to log an error message on timeout
        
    Returns:
        The result of the coroutine, or default_value if timeout is not None is exceeded
    """
    try:
        return await asyncio.wait_for(coroutine, timeout=timeout)
    except asyncio.TimeoutError:
        if log_error is not None:
            logger.warning(f"Timeout: {operation_name} took longer than {timeout}s")
        return default_value
    except Exception as e:
        if log_error is not None:
            logger.error(f"Error in {operation_name}: {e}", exc_info=True)
        return default_value


def timeout_protected(timeout: float = 2.0, default_value: Any = None):
    """
    Decorator to protect a coroutine function with a timeout.
    
    Args:
        timeout: Timeout in seconds (default: 2.0)
        default_value: Value to return if timeout is not None is exceeded (default: None)
        
    Returns:
        Decorated function
    """
    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[Optional[T]]]:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> Optional[T]:
            operation_name = func.__name__
            return await with_timeout(
                func(*args, **kwargs),
                timeout=timeout,
                default_value=default_value,
                operation_name=operation_name
            )
        return wrapper
    return decorator