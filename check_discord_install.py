"""
Simple test script to verify discord (py-cord) imports are working correctly
"""
import sys
import importlib
import os

def check_module_location(module_name):
    """Check where a module is being loaded from"""
    try:
        module = importlib.import_module(module_name)
        file_path = getattr(module, '__file__', 'Unknown location')
        return f"{module_name} is loaded from: {file_path}"
    except ImportError as e:
        return f"Cannot import {module_name}: {e}"

def check_installed_packages():
    """Check what discord-related packages are installed"""
    print("Checking installed packages...")
    try:
        import importlib.metadata
        discord_packages = [d for d in importlib.metadata.distributions() 
                         if 'discord' in d.metadata["Name"].lower()]
        
        for pkg in discord_packages:
            print(f"- {pkg.metadata['Name']} version {pkg.version}")
            try:
                pkg_location = pkg.locate_file('')
                print(f"  Location: {pkg_location}")
            except Exception as e:
                print(f"  Error finding location: {e}")
    except Exception as e:
        print(f"Error checking packages: {e}")
        
def test_imports():
    """Test importing key discord modules"""
    print("\nTesting imports...")
    
    # Test main discord module
    try:
        import discord
        print(f"✓ Successfully imported discord - version: {getattr(discord, '__version__', 'unknown')}")
    except ImportError as e:
        print(f"✗ Failed to import discord: {e}")
        return False
    
    # Test ext.commands
    try:
        from discord.ext import commands
        print(f"✓ Successfully imported discord.ext.commands")
    except ImportError as e:
        print(f"✗ Failed to import discord.ext.commands: {e}")
    
    # Test app_commands
    try:
        from discord import app_commands
        print(f"✓ Successfully imported discord.app_commands")
    except ImportError as e:
        print(f"✗ Failed to import discord.app_commands: {e}")
    
    # Test bridge (py-cord specific)
    try:
        from discord.ext import bridge
        print(f"✓ Successfully imported discord.ext.bridge (confirms py-cord)")
    except ImportError as e:
        print(f"✗ Failed to import discord.ext.bridge: {e}")
    
    # Check for discord.ui (modern UI components)
    try:
        from discord import ui
        print(f"✓ Successfully imported discord.ui")
    except ImportError as e:
        print(f"✗ Failed to import discord.ui: {e}")
    
    return True

def check_paths():
    """Check Python import paths"""
    print("\nPython sys.path:")
    for i, path in enumerate(sys.path):
        print(f"{i}: {path}")

def main():
    print("=== Discord Module Installation Check ===\n")
    
    # Check what packages are installed
    check_installed_packages()
    
    # Check Python import paths
    check_paths()
    
    # Test importing key modules
    success = test_imports()
    
    # Check specific module locations
    print("\nChecking module locations:")
    modules_to_check = [
        'discord', 
        'discord.ext.commands', 
        'discord.app_commands',
        'discord.ext.bridge'  # py-cord specific
    ]
    
    for module in modules_to_check:
        print(check_module_location(module))
    
    # Overall result
    print("\n=== Summary ===")
    if success:
        print("Discord module (py-cord) appears to be working correctly!")
    else:
        print("There are issues with the discord module installation")

if __name__ == "__main__":
    main()