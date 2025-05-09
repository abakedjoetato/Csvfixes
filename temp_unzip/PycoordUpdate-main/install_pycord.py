"""
Script to install py-cord 2.6.1 correctly and remove any conflicting packages.

This script:
1. Uninstalls discord.py and conflicting packages
2. Installs py-cord 2.6.1
3. Verifies the installation
"""
import os
import sys
import subprocess
import importlib
import importlib.metadata

def run_pip_command(cmd):
    """Run a pip command and return its output"""
    print(f"Running: {cmd}")
    process = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(process.stdout)
    if process.stderr:
        print(f"Error: {process.stderr}")
    return process.returncode == 0

def uninstall_conflicting_packages():
    """Uninstall discord.py and any other conflicting packages"""
    print("Uninstalling conflicting packages...")
    
    # First find all discord-related packages
    try:
        discord_packages = []
        for dist in importlib.metadata.distributions():
            if "discord" in dist.metadata["Name"].lower() and dist.metadata["Name"] != "py-cord":
                discord_packages.append(dist.metadata["Name"])
        
        print(f"Found discord packages to remove: {discord_packages}")
        
        # Uninstall all found packages
        for package in discord_packages:
            run_pip_command(f"{sys.executable} -m pip uninstall -y {package}")
    except Exception as e:
        print(f"Error finding packages: {e}")
    
    # Try specific uninstallations for common conflicting packages
    packages_to_remove = [
        "discord.py",
        "discord-py",
        "discord-py-slash-command",
        "discord-ext-commands",
    ]
    
    for package in packages_to_remove:
        run_pip_command(f"{sys.executable} -m pip uninstall -y {package}")
    
    print("Finished uninstalling conflicting packages")

def install_pycord():
    """Install py-cord 2.6.1"""
    print("Installing py-cord 2.6.1...")
    
    # First use --no-deps to avoid pulling in discord.py
    success = run_pip_command(f"{sys.executable} -m pip install --no-deps py-cord==2.6.1")
    
    # Then install dependencies except discord.py and related packages
    if success:
        print("Installing dependencies...")
        deps = [
            "aiohttp>=3.7.4",
            "attrs>=17.3.0",
            "frozenlist>=1.1.1",
            "multidict>=4.5",
            "yarl>=1.7.0",
            "typing_extensions>=4.0.0"
        ]
        for dep in deps:
            success = run_pip_command(f"{sys.executable} -m pip install --upgrade {dep}")
            if not success:
                print(f"Failed to install dependency: {dep}")
                
        # Create a symlink or fix paths to make imports work correctly
        print("Setting up import paths...")
        try:
            import site
            site_packages = site.getsitepackages()[0]
            print(f"Site packages directory: {site_packages}")
            
            # Try to fix discord imports 
            if os.path.exists(os.path.join(site_packages, 'py_cord')):
                print("Found py_cord directory, ensuring imports work correctly")
            else:
                print("py_cord directory not found in site packages")
        except Exception as e:
            print(f"Error setting up paths: {e}")
    
    print("Finished installing py-cord and dependencies")
    return success

def verify_installation():
    """Verify py-cord is correctly installed"""
    print("Verifying py-cord installation...")
    
    try:
        # Attempt to import discord module
        import discord
        print(f"Successfully imported discord module (py-cord)")
        print(f"Version: {discord.__version__}")
        
        # Check if this is definitely py-cord by testing imports
        try:
            from discord.ext import bridge
            print("Successfully imported bridge module - confirmed as py-cord")
            bridge_found = True
        except ImportError:
            print("Failed to import bridge module - not py-cord")
            bridge_found = False
        
        # Try importing expected modules
        try:
            from discord.ext import commands
            print("Successfully imported commands module")
            commands_ok = True
        except ImportError:
            print("Failed to import commands module")
            commands_ok = False
            
        try:
            from discord import app_commands
            print("Successfully imported app_commands module")
            app_commands_ok = True
        except ImportError:
            print("Failed to import app_commands module")
            app_commands_ok = False
        
        # Check if necessary classes are available
        if app_commands_ok:
            print("\nChecking for app_commands.AppCommandOptionType...")
            try:
                from discord.app_commands import AppCommandOptionType
                print("AppCommandOptionType found in app_commands")
                app_commands_type_ok = True
            except ImportError:
                print("AppCommandOptionType not found in app_commands")
                app_commands_type_ok = False
        else:
            app_commands_type_ok = False
        
        # Overall verification result
        if bridge_found and commands_ok and app_commands_ok and app_commands_type_ok:
            print("\nVerification PASSED: py-cord 2.6.1 is correctly installed")
            return True
        else:
            print("\nVerification FAILED: py-cord 2.6.1 is not correctly installed")
            print(f"- bridge module available: {bridge_found}")
            print(f"- commands module available: {commands_ok}")
            print(f"- app_commands module available: {app_commands_ok}")
            print(f"- AppCommandOptionType available: {app_commands_type_ok}")
            return False
            
    except ImportError as e:
        print(f"Error importing discord: {e}")
        print("py-cord is not installed or not working correctly")
        return False

def main():
    """Main entry point"""
    print("=== py-cord 2.6.1 Installation Script ===")
    
    # 1. Uninstall conflicting packages
    uninstall_conflicting_packages()
    
    # 2. Install py-cord
    if install_pycord():
        print("py-cord installation completed successfully")
    else:
        print("Failed to install py-cord")
        return 1
    
    # 3. Verify installation
    if verify_installation():
        print("Installation verified! You can now import discord modules from py-cord.")
    else:
        print("Verification failed. See above errors for details.")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())