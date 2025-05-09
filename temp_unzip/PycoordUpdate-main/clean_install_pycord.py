"""
Clean installer for py-cord 2.6.1 without discord.py
"""
import subprocess
import sys
import os
import time

def run_command(cmd):
    """Run a shell command and return the output and success status"""
    print(f"Running: {cmd}")
    process = subprocess.run(
        cmd, 
        shell=True, 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE,
        text=True
    )
    
    if process.returncode != 0:
        print(f"Error: {process.stderr}")
        return False, process.stdout + "\n" + process.stderr
    
    return True, process.stdout

def main():
    """Main installer function"""
    print("=== Clean py-cord 2.6.1 Installer ===")
    
    # Step 1: Uninstall any existing discord libraries
    packages_to_remove = [
        "discord.py",
        "discord-py",
        "py-cord",
        "discord-py-slash-command"
    ]
    
    print("Removing existing discord packages...")
    for pkg in packages_to_remove:
        run_command(f"{sys.executable} -m pip uninstall -y {pkg}")
    
    # Wait a bit for uninstall to finalize
    time.sleep(2)
    
    # Step 2: Install py-cord with specific version directly from GitHub
    print("\nInstalling py-cord 2.6.1...")
    success, output = run_command(f"{sys.executable} -m pip install --no-deps py-cord==2.6.1")
    
    if not success:
        print("Failed to install py-cord.")
        return 1
    
    # Step 3: Install py-cord dependencies manually (except discord.py)
    print("\nInstalling py-cord dependencies...")
    dependencies = [
        "aiohttp>=3.7.4",
        "typing_extensions>=4.0.0"
    ]
    
    for dep in dependencies:
        run_command(f"{sys.executable} -m pip install {dep}")
    
    # Step 4: Verify installation
    print("\nVerifying installation...")
    try:
        # Try importing discord
        import importlib
        discord = importlib.import_module("discord")
        print(f"Successfully imported discord module (py-cord)")
        print(f"Version: {getattr(discord, '__version__', 'unknown')}")
        
        # Test if this is py-cord by trying bridge import
        try:
            from discord.ext import bridge
            print("Successfully imported bridge module - confirmed as py-cord!")
            bridge_found = True
        except ImportError:
            print("WARNING: Failed to import bridge module - this might not be py-cord")
            bridge_found = False
            
        # Try importing app_commands
        try:
            from discord import app_commands
            print("Successfully imported app_commands module")
            app_commands_found = True
        except ImportError:
            print("WARNING: Failed to import app_commands module")
            app_commands_found = False
            
        if bridge_found and app_commands_found:
            print("\nVERIFICATION PASSED: py-cord 2.6.1 installed correctly")
            return 0
        else:
            print("\nVERIFICATION FAILED: py-cord may not be installed correctly")
            return 1
            
    except ImportError as e:
        print(f"ERROR: Failed to import discord module: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())