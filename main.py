"""
Emeralds Killfeed PvP Statistics Discord Bot
Main entry point for Replit run button - runs the Discord bot directly
as required by rule #7 in rules.md (Stack Integrity Is Mandatory)
"""
import os
import sys
import logging
import asyncio
import traceback
import signal
import time
import psutil
import gc
from datetime import datetime

# Configure more detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bot.log')
    ]
)
logger = logging.getLogger('main')

# Set higher log level for some verbose libraries
logging.getLogger('discord.gateway').setLevel(logging.WARNING)
logging.getLogger('discord.client').setLevel(logging.WARNING)
logging.getLogger('discord.http').setLevel(logging.WARNING)

# Create a flag file to indicate we're running in a workflow
with open(".running_in_workflow", "w") as f:
    f.write(f"Started at {datetime.now()}")

# Track when we last restarted
def record_restart():
    try:
        with open("restart_log.txt", "a") as f:
            f.write(f"{datetime.now()}: Bot restarted\n")
    except Exception as e:
        logger.error(f"Failed to record restart: {e}")

# Log system resource usage
def log_system_resources():
    try:
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        cpu_percent = process.cpu_percent(interval=0.1)
        
        logger.info(f"System Resources: Memory RSS={memory_info.rss/1024/1024:.2f}MB, "
                   f"VMS={memory_info.vms/1024/1024:.2f}MB, CPU={cpu_percent:.1f}%")
        
        # Log number of active tasks
        try:
            all_tasks = asyncio.all_tasks() if hasattr(asyncio, 'all_tasks') else []
            logger.info(f"Active asyncio tasks: {len(all_tasks)}")
            
            # Log pending tasks (limited to first 5)
            pending_tasks = [t for t in all_tasks if not t.done()]
            if pending_tasks:
                task_names = [t.get_name() for t in pending_tasks[:5]]
                logger.info(f"Sample pending tasks (first 5): {task_names}")
                
        except Exception as task_err:
            logger.error(f"Error getting asyncio tasks: {task_err}")
            
    except Exception as e:
        logger.error(f"Failed to log system resources: {e}")

# Check for event loop problems
def check_event_loop():
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            logger.error("Main event loop is closed, this is unexpected")
        else:
            logger.info("Main event loop is operational")
    except Exception as e:
        logger.error(f"Error checking event loop: {e}")

# Handle signals gracefully
def signal_handler(signum, frame):
    sig_name = signal.Signals(signum).name
    logger.warning(f"Received signal {sig_name} ({signum})")
    if signum in (signal.SIGINT, signal.SIGTERM):
        logger.info("Bot stopping due to termination signal")
        sys.exit(0)

# Watchdog function to monitor bot health - lightweight version
def start_watchdog():
    def _watchdog():
        last_check = time.time()
        last_gc_time = 0
        last_resource_log = 0
        high_memory_count = 0  # Track consecutive high memory readings
        
        # Use less frequent checks to reduce overhead
        while True:
            try:
                # Sleep in shorter intervals to allow cleaner thread exit
                time.sleep(15)
                
                current_time = time.time()
                elapsed = current_time - last_check
                
                # Check memory usage every interval
                process = psutil.Process()
                memory_info = process.memory_info()
                memory_mb = memory_info.rss / 1024 / 1024
                
                # Perform emergency garbage collection if memory usage is very high
                if memory_mb > 800:  # Critical threshold: 800MB
                    logger.warning(f"CRITICAL: Memory usage very high at {memory_mb:.2f}MB, performing emergency cleanup")
                    objects = gc.collect(generation=2)  # Full collection
                    logger.info(f"Emergency garbage collection: {objects} objects collected")
                    
                    # If memory is still critically high after cleanup, consider restarting
                    high_memory_count += 1
                    if high_memory_count >= 3:  # Three consecutive high readings
                        logger.critical(f"Memory usage persistently high ({memory_mb:.2f}MB) after cleanup")
                        # Log memory allocation for diagnostics
                        try:
                            import tracemalloc
                            if not tracemalloc.is_tracing():
                                tracemalloc.start()
                            snapshot = tracemalloc.take_snapshot()
                            top_stats = snapshot.statistics('lineno')
                            logger.critical("Top memory allocations:")
                            for stat in top_stats[:10]:  # Show top 10 allocations
                                logger.critical(f"{stat}")
                        except ImportError:
                            logger.warning("tracemalloc not available for memory diagnostics")
                        except Exception as e:
                            logger.error(f"Error in memory diagnostics: {e}")
                        
                elif memory_mb > 500:  # Warning threshold: 500MB
                    logger.warning(f"Memory usage high at {memory_mb:.2f}MB, collecting garbage")
                    objects = gc.collect(generation=1)  # Collect middle generation
                    logger.info(f"Targeted garbage collection: {objects} objects collected")
                    high_memory_count = max(0, high_memory_count - 1)  # Decrease counter if below critical
                else:
                    high_memory_count = 0  # Reset counter when memory usage is normal
                
                # Log status every minute
                if current_time - last_resource_log >= 60:
                    logger.info(f"Watchdog running, elapsed since start: {elapsed:.1f}s")
                    log_system_resources()
                    last_resource_log = current_time
                    
                    # Check asyncio tasks periodically
                    try:
                        loop = asyncio.get_event_loop()
                        tasks = asyncio.all_tasks(loop)
                        logger.info(f"Active asyncio tasks: {len(tasks)}")
                    except RuntimeError:
                        logger.error("Error getting asyncio tasks: no running event loop")
                    except Exception as e:
                        logger.error(f"Error getting asyncio tasks: {e}")
                
                # Run regular garbage collection every 3 minutes
                if current_time - last_gc_time >= 180:
                    objects = gc.collect()
                    logger.info(f"Garbage collection: {objects} objects collected")
                    last_gc_time = current_time
                    
                last_check = current_time
                
            except Exception as e:
                logger.error(f"Error in watchdog: {e}")
                # Sleep a bit after an error to prevent tight error loops
                time.sleep(30)
                
    # Start watchdog in a separate thread with lower memory impact
    import threading
    watchdog_thread = threading.Thread(target=_watchdog, daemon=True, name="bot_watchdog")
    watchdog_thread.start()
    logger.info("Started lightweight watchdog thread")
    return watchdog_thread

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

if __name__ == "__main__":
    # Record this restart
    record_restart()
    
    # Print a banner to make it clear the bot is starting
    print("=" * 60)
    print("  Emeralds Killfeed PvP Statistics Discord Bot")
    print("=" * 60)
    print(f"  Starting bot at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("  Press Ctrl+C to stop the bot")
    print("=" * 60)
    
    # Start monitoring
    watchdog = start_watchdog()
    
    # Log initial system state
    log_system_resources()
    check_event_loop()
    
    logger.info("Starting Emeralds Killfeed PvP Statistics Discord Bot")
    try:
        # Import and run the bot
        from bot import main as bot_main
        exit_code = bot_main()
        logger.info(f"Bot exited with code: {exit_code}")
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except asyncio.CancelledError:
        logger.error("Bot main task was cancelled unexpectedly")
        sys.exit(2)
    except Exception as e:
        logger.error(f"Error starting bot: {e}", exc_info=True)
        logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)