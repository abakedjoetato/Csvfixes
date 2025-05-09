"""
Check which discord module is active in the current environment
"""
import sys
import os
import importlib.util

def print_module_details(module_name):
    """Print details about an imported module"""
    try:
        spec = importlib.util.find_spec(module_name)
        if spec is None:
            print(f"- {module_name}: NOT FOUND")
            return False
            
        module = importlib.import_module(module_name)
        
        version = getattr(module, "__version__", "unknown")
        location = getattr(module, "__file__", "unknown location")
        
        print(f"- {module_name}:")
        print(f"  Version: {version}")
        print(f"  Location: {location}")
        
        return True
    except Exception as e:
        print(f"- {module_name}: ERROR - {e}")
        return False

def test_discord_imports():
    """Test critical discord imports"""
    print("\nTesting critical imports...")
    
    # Main discord module
    main_imported = False
    try:
        import discord
        main_imported = True
        print(f"✓ discord - version: {getattr(discord, '__version__', 'unknown')}")
        
        # Check if it's py-cord by testing various methods
        is_pycord = False
        
        # Method 1: Check bridge existence (safer than importing)
        import pkgutil
        bridge_exists = pkgutil.find_loader('discord.ext.bridge') is not None
        print(f"Bridge module exists: {bridge_exists}")
        if bridge_exists:
            is_pycord = True
            
        # Method 2: Check module structure
        try:
            import inspect
            source_file = inspect.getsourcefile(discord)
            print(f"Main discord source file: {source_file}")
            
            # Check if py_cord is in the path
            if 'py_cord' in source_file:
                print("py_cord found in module path")
                is_pycord = True
        except Exception as e:
            print(f"Error checking source: {e}")
            
        # Method 3: Check known py-cord attributes
        has_slash_command = hasattr(discord.ext.commands, 'slash_command')
        print(f"Has slash_command: {has_slash_command}")
        if has_slash_command:
            is_pycord = True
            
        # Final determination
        if is_pycord:
            print("This appears to be py-cord")
        else:
            print("This appears to be discord.py")
    except ImportError as e:
        print(f"✗ discord - {e}")
    
    if not main_imported:
        return
        
    # Test common imports needed for the project
    imports_to_test = [
        "discord.ext.commands",
        "discord.app_commands", 
        "discord.ext.tasks",
        "discord.ui"
    ]
    
    for imp in imports_to_test:
        try:
            module = importlib.import_module(imp)
            print(f"✓ {imp}")
        except ImportError as e:
            print(f"✗ {imp} - {e}")

def main():
    """Main function"""
    print("=== Discord Environment Check ===\n")
    
    print("Python version:", sys.version)
    print("Python executable:", sys.executable)
    
    # Check for discord-related installed packages
    print("\nInstalled discord-related packages:")
    try:
        import importlib.metadata
        discord_packages = [dist for dist in importlib.metadata.distributions() 
                         if 'discord' in dist.metadata["Name"].lower()]
        
        for pkg in discord_packages:
            print(f"- {pkg.metadata['Name']} version {pkg.version}")
    except Exception as e:
        print(f"Error checking packages: {e}")
    
    # Check discord module details
    print("\nModule details:")
    print_module_details("discord")
    print_module_details("discord.ext.commands")
    print_module_details("discord.app_commands")
    
    # Test imports
    test_discord_imports()
    
    print("\nNamespace conflicts:")
    try:
        import discord
        print(f"discord.__name__ = {discord.__name__}")
        print(f"discord.__package__ = {discord.__package__}")
        
        # Check for bridge (py-cord specific)
        try:
            from discord.ext import bridge
            print("bridge exists - likely py-cord")
        except ImportError:
            print("bridge doesn't exist - likely discord.py")
        
        # Check AppCommandOptionType location 
        try:
            from discord.app_commands import AppCommandOptionType
            print("AppCommandOptionType in app_commands - newer structure")
        except ImportError:
            try:
                from discord import AppCommandOptionType
                print("AppCommandOptionType in discord - older structure")
            except ImportError:
                print("AppCommandOptionType not found")
    except Exception as e:
        print(f"Error checking namespace conflicts: {e}")
    
    print("\n=== Check Complete ===")

if __name__ == "__main__":
    main()