#!/usr/bin/env python3
"""
Robust wrapper script for the Discord bot with auto-restart capabilities.
This script provides:
1. Error handling for crashes and exceptions
2. Automatic restart with increasing backoff on failure
3. Detailed logging of crash causes
4. Memory usage monitoring
"""
import os
import sys
import time
import logging
import subprocess
import signal
import psutil
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bot_wrapper.log')
    ]
)
logger = logging.getLogger('bot_wrapper')

# Constants
MAX_RESTART_DELAY = 300  # 5 minutes max backoff
MAX_RESTARTS_IN_WINDOW = 5  # Maximum number of restarts in time window
RESTART_WINDOW = 600  # 10 minute window for restart tracking

# Track restarts
restart_history = []
current_delay = 1  # Start with 1 second delay

def log_restart(message):
    """Log restart to restart_log.txt and screen"""
    timestamp = datetime.now()
    restart_history.append(timestamp)
    
    # Log to screen
    logger.info(f"{message} at {timestamp}")
    
    # Log to restart file
    try:
        with open("restart_log.txt", "a") as f:
            f.write(f"{timestamp}: {message}\n")
    except Exception as e:
        logger.error(f"Failed to record restart: {e}")

def check_restart_rate():
    """Check if we're restarting too frequently, indicating serious issues"""
    global restart_history
    
    # Clean old restart records
    cutoff_time = datetime.now() - timedelta(seconds=RESTART_WINDOW)
    restart_history = [ts for ts in restart_history if ts > cutoff_time]
    
    # Check if we have too many restarts in our window
    if len(restart_history) > MAX_RESTARTS_IN_WINDOW:
        logger.critical(f"Too many restarts ({len(restart_history)}) in {RESTART_WINDOW/60} minute window!")
        logger.critical("Pausing for extended period to prevent crash loop")
        return True
    
    return False

def get_memory_usage():
    """Get current memory usage of the process"""
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    return {
        'rss': memory_info.rss / 1024 / 1024,  # MB
        'vms': memory_info.vms / 1024 / 1024,  # MB
    }

def signal_handler(signum, frame):
    """Handle termination signals properly"""
    sig_name = signal.Signals(signum).name
    logger.info(f"Received signal {sig_name} ({signum})")
    
    if signum in (signal.SIGINT, signal.SIGTERM):
        logger.info("Wrapper stopping due to termination signal")
        sys.exit(0)

def check_memory_usage():
    """Check and report high memory usage"""
    try:
        memory = get_memory_usage()
        
        # Only log memory usage if it seems excessive
        if memory['rss'] > 500:  # >500MB is concerning
            logger.warning(f"High memory usage detected: {memory['rss']:.1f} MB")
            
            # Try to get a detailed memory report
            try:
                import tracemalloc
                
                if not tracemalloc.is_tracing():
                    tracemalloc.start()
                
                # Let it collect some data
                time.sleep(1)
                
                # Take snapshot and analyze
                snapshot = tracemalloc.take_snapshot()
                top_stats = snapshot.statistics('lineno')
                
                logger.warning("Top 10 memory allocations:")
                for i, stat in enumerate(top_stats[:10]):
                    logger.warning(f"#{i+1}: {stat}")
                    
                # Emergency garbage collection
                import gc
                collected = gc.collect()
                logger.info(f"Emergency garbage collection freed {collected} objects")
                
            except ImportError:
                logger.warning("tracemalloc not available for memory diagnostics")
            except Exception as e:
                logger.error(f"Error in memory diagnostics: {e}")
                
            return True
            
        return False
    except:
        return False

def run_bot():
    """Run the Discord bot with proper monitoring"""
    global current_delay
    
    # Print header
    print("=" * 60)
    print("  Discord Bot Launcher with Stability Wrapper")
    print("=" * 60)
    print(f"  Starting bot at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("  Press Ctrl+C to stop the bot")
    print("=" * 60)
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create a flag file to indicate we're running in a workflow
    with open(".running_in_workflow", "w") as f:
        f.write(f"Started at {datetime.now()}")
        
    # Create a monitoring file for diagnostics
    with open("wrapper_status.txt", "w") as f:
        f.write(f"Wrapper started at {datetime.now()}\n"
                f"Python version: {sys.version}\n"
                f"Initial memory: {get_memory_usage()['rss']:.1f} MB\n")
    
    # Main restart loop
    while True:
        # Check if we need an extended pause due to restart rate
        if check_restart_rate():
            logger.warning(f"Entering extended cooldown period ({MAX_RESTART_DELAY*2} seconds)")
            time.sleep(MAX_RESTART_DELAY * 2)  # Double the max delay for cooldown
            current_delay = 1  # Reset delay after cooldown
        
        log_restart("Bot restarted")
        
        start_time = time.time()
        logger.info(f"Starting bot process (memory: {get_memory_usage()['rss']:.1f} MB)")
        
        # Check memory before launch
        if check_memory_usage():
            logger.warning("Performing pre-launch memory cleanup due to high usage")
            time.sleep(2)  # Brief pause for cleanup effects
            
        try:
            # Run the main.py script
            result = subprocess.run([sys.executable, "main.py"], 
                                  check=True, 
                                  text=True, 
                                  capture_output=False)
            
            # Check memory after run completes
            high_memory = check_memory_usage()
            
            # If we exit normally, reset the backoff delay
            logger.info(f"Bot process exited with code {result.returncode}")
            
            # If memory was high, don't immediately reset the delay to prevent rapid restarts
            if not high_memory:
                current_delay = 1
            
            # If the bot requested termination, respect it
            if result.returncode == 0:
                logger.info("Bot exited cleanly, restarting...")
                time.sleep(1)  # Short delay for clean restart
            else:
                logger.warning(f"Bot exited with error code {result.returncode}, restarting with delay...")
                time.sleep(current_delay)
                current_delay = min(current_delay * 2, MAX_RESTART_DELAY)  # Exponential backoff
                
        except subprocess.CalledProcessError as e:
            runtime = time.time() - start_time
            logger.error(f"Bot crashed with return code {e.returncode} after {runtime:.1f} seconds")
            
            # Apply backoff delay based on how quickly it failed
            if runtime < 10:  # If it failed very quickly
                current_delay = min(current_delay * 2, MAX_RESTART_DELAY)  # Increase backoff faster
            else:
                current_delay = min(current_delay * 1.5, MAX_RESTART_DELAY)  # Slower backoff for longer runs
                
            logger.info(f"Restarting in {current_delay:.1f} seconds (memory: {get_memory_usage()['rss']:.1f} MB)")
            time.sleep(current_delay)
            
        except KeyboardInterrupt:
            logger.info("Bot stopped by user (Ctrl+C)")
            break
            
        except Exception as e:
            logger.error(f"Unexpected error running bot: {e}")
            import traceback
            traceback.print_exc()
            
            # More aggressive backoff for unexpected errors
            current_delay = min(current_delay * 3, MAX_RESTART_DELAY)
            logger.info(f"Restarting in {current_delay:.1f} seconds (memory: {get_memory_usage()['rss']:.1f} MB)")
            time.sleep(current_delay)

if __name__ == "__main__":
    try:
        run_bot()
    except KeyboardInterrupt:
        logger.info("Wrapper stopped by user")
    except Exception as e:
        logger.critical(f"Fatal error in wrapper: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)