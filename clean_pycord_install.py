"""
Clean installation script for py-cord 2.6.1 only, without discord.py
"""
import os
import sys
import subprocess
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('clean_pycord_install')

def run_command(cmd):
    """Run a shell command and return the output and success status"""
    logger.info(f"Running command: {cmd}")
    try:
        result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        return result.stdout, True
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed with error: {e.stderr}")
        return e.stderr, False

def main():
    """Main installer function"""
    # Step 1: List all installed discord-related packages
    logger.info("Checking currently installed discord packages...")
    out, success = run_command("pip list | grep -i discord")
    if success and out:
        logger.info(f"Found discord packages:\n{out}")
    
    # Step 2: Force uninstall all discord-related packages
    logger.info("Uninstalling all discord-related packages...")
    packages_to_uninstall = [
        "discord.py", "discord-py", "py-cord", "discord", 
        "discord-py-slash-command", "discord-py-interactions"
    ]
    
    for package in packages_to_uninstall:
        out, success = run_command(f"pip uninstall -y {package}")
        if success:
            logger.info(f"Successfully uninstalled {package}")
        else:
            logger.info(f"Package {package} was not installed")
    
    # Step 3: Install only py-cord 2.6.1
    logger.info("Installing py-cord 2.6.1...")
    out, success = run_command("pip install py-cord==2.6.1")
    
    if not success:
        logger.error("Failed to install py-cord 2.6.1. Exiting.")
        return 1
    
    logger.info("py-cord 2.6.1 installation successful")
    
    # Step 4: Block discord.py from being installed
    logger.info("Adding constraints to prevent discord.py installation...")
    constraints_content = "discord.py\ndiscord-py\n"
    
    with open("constraints.txt", "w") as f:
        f.write(constraints_content)
    
    logger.info("Created constraints.txt to prevent discord.py installation")
    
    # Step 5: Verify the installation
    logger.info("Verifying py-cord installation...")
    out, success = run_command("pip list | grep -i discord")
    
    if success and out:
        logger.info(f"Installed discord packages:\n{out}")
    
    # Step 6: Simple import test
    logger.info("Testing imports...")
    try:
        import discord
        from discord.ext import commands
        from discord.ext.commands import Bot
        from discord import app_commands
        
        logger.info(f"discord.__version__: {discord.__version__}")
        logger.info(f"discord path: {discord.__file__}")
        logger.info("Import test successful!")
        
        return 0
    except ImportError as e:
        logger.error(f"Import test failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())