"""
Script to analyze discord module conflicts and identify which library is being used.
"""
import sys
import os
import importlib.util
import importlib.metadata
import pkgutil

def check_module_exists(module_name):
    """Check if a module exists and return its location"""
    try:
        spec = importlib.util.find_spec(module_name)
        if spec:
            return True, spec.origin
        return False, None
    except (ModuleNotFoundError, AttributeError):
        return False, None

def find_all_installations(module_prefix):
    """Find all installed packages with the given prefix"""
    return [pkg for pkg in importlib.metadata.distributions() 
            if pkg.metadata["Name"].startswith(module_prefix)]

def get_module_version(module_name):
    """Get the version of a module if it exists"""
    try:
        module = importlib.import_module(module_name)
        return getattr(module, "__version__", "Unknown")
    except (ModuleNotFoundError, ImportError):
        return "Not Found"

def print_module_path(module_name):
    """Print the file path of a module"""
    exists, path = check_module_exists(module_name)
    if exists:
        print(f"Module {module_name} exists at: {path}")
    else:
        print(f"Module {module_name} does not exist")

def check_import_behavior():
    """Check which package is used when importing discord"""
    try:
        import discord
        version = getattr(discord, "__version__", "Unknown")
        path = getattr(discord, "__file__", "Unknown")
        
        print(f"When 'import discord' is used:")
        print(f"  - Version: {version}")
        print(f"  - Path: {path}")
        
        # Check if it looks like discord.py or py-cord
        if hasattr(discord, "Client"):
            print("  - Library appears to be discord.py (or compatible)")
        else:
            print("  - Library may not be discord.py compatible")
            
        # Check for specific modules that might indicate which library we're using
        modules_to_check = [
            "discord.ext.commands",
            "discord.app_commands", 
            "discord.ui",
            "discord.abc"
        ]
        
        for module in modules_to_check:
            exists, path = check_module_exists(module)
            if exists:
                print(f"  - {module}: Available at {path}")
            else:
                print(f"  - {module}: Not available")
                
    except ImportError:
        print("Could not import discord module")

def search_site_packages():
    """Search site-packages directory for discord and py-cord packages"""
    print("\nSearching site-packages directories:")
    for path in sys.path:
        if "site-packages" in path:
            print(f"\nDirectory: {path}")
            try:
                contents = os.listdir(path)
                discord_items = [item for item in contents if "discord" in item.lower() or "cord" in item.lower()]
                
                for item in discord_items:
                    item_path = os.path.join(path, item)
                    if os.path.isdir(item_path):
                        print(f"- Directory: {item}")
                        try:
                            subcontents = os.listdir(item_path)
                            print(f"  Contents: {subcontents[:5]} ... (truncated)")
                        except Exception as e:
                            print(f"  Error reading directory: {e}")
                    else:
                        print(f"- File: {item} ({os.path.getsize(item_path)} bytes)")
            except Exception as e:
                print(f"Error reading directory: {e}")

def main():
    """Main function to check discord module installations"""
    print("=" * 50)
    print("Discord Module Conflict Analysis")
    print("=" * 50)
    
    # Check installed packages
    print("\nInstalled packages:")
    discord_packages = find_all_installations("discord")
    pycord_packages = find_all_installations("py-cord")
    
    for pkg in discord_packages:
        print(f"- {pkg.metadata['Name']} {pkg.version}")
    
    for pkg in pycord_packages:
        print(f"- {pkg.metadata['Name']} {pkg.version}")
    
    # Check import paths
    print("\nModule paths:")
    print_module_path("discord")
    print_module_path("discord.ext")
    print_module_path("discord.ext.commands")
    print_module_path("discord.app_commands")
    
    # Check import behavior
    print("\nImport behavior:")
    check_import_behavior()
    
    # Search site-packages directory
    search_site_packages()
    
    # Check discord namespace content
    print("\nDiscord namespace content:")
    try:
        import discord
        dir_items = dir(discord)
        important_items = [
            "Client", "Bot", "AppCommandOptionType", "Intents", 
            "app_commands", "ext", "ui", "slash_command"
        ]
        
        for item in important_items:
            if item in dir_items:
                print(f"- {item}: Present")
            else:
                print(f"- {item}: Missing")
                
    except ImportError:
        print("Could not import discord module")

if __name__ == "__main__":
    main()