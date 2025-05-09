#!/usr/bin/env python3
"""
Script to check py-cord installation structure

This script:
1. Checks if py-cord is correctly installed
2. Lists available imports and modules
3. Identifies which imports match py-cord 2.6.1 expectations
"""

import sys
import pkgutil
import importlib
import os.path
from pprint import pprint

def check_module_exists(module_path):
    """Check if a module exists and whether it can be imported"""
    try:
        module = importlib.import_module(module_path)
        print(f"✓ Module {module_path} exists and can be imported")
        return module
    except ImportError as e:
        print(f"✗ Cannot import {module_path}: {e}")
        return None

def explore_package(package_name, depth=0, max_depth=2):
    """Explore a package structure recursively"""
    if depth > max_depth:
        return
    
    try:
        package = importlib.import_module(package_name)
        print(f"{'  ' * depth}Package: {package_name}")
        
        # Try to get __version__
        if hasattr(package, '__version__'):
            print(f"{'  ' * depth}Version: {package.__version__}")
            
        # Try to get __path__
        if hasattr(package, '__path__'):
            print(f"{'  ' * depth}Path: {package.__path__}")
            
            # List all modules in the package
            for _, name, is_pkg in pkgutil.iter_modules(package.__path__, package.__name__ + '.'):
                if is_pkg:
                    explore_package(name, depth + 1, max_depth)
                else:
                    print(f"{'  ' * (depth+1)}Module: {name}")
        else:
            print(f"{'  ' * depth}No __path__ attribute")
            
    except ImportError as e:
        print(f"{'  ' * depth}Cannot import {package_name}: {e}")

def main():
    """Main entry point"""
    print("Checking py-cord installation...\n")
    
    # Check if discord module is available
    discord = check_module_exists('discord')
    if discord:
        if hasattr(discord, '__file__') and discord.__file__:
            print(f"Discord package location: {os.path.dirname(discord.__file__)}")
        else:
            print("Discord package has no __file__ attribute or it is None")
    
    # Check for key modules
    critical_modules = [
        'discord.abc',
        'discord.ext',
        'discord.ext.commands',
        'discord.app_commands',
        'discord.bot'
    ]
    
    print("\nChecking critical modules:")
    for module in critical_modules:
        check_module_exists(module)
    
    # Explore discord package structure
    print("\nExploring discord package structure:")
    explore_package('discord')
    
    # Check if both discord.py and py-cord are installed
    print("\nChecking for package conflicts:")
    try:
        import pip._internal.metadata as metadata
        environment = metadata.Environment.default()
        installed_packages = list(environment.iter_installed_distributions())
        discord_related = [pkg for pkg in installed_packages if 'discord' in pkg.name.lower() or 'py-cord' in pkg.name.lower()]
        if discord_related:
            print("Discord-related packages installed:")
            for pkg in discord_related:
                print(f"  - {pkg.name} {pkg.version}")
                if hasattr(pkg, 'requires'):
                    print(f"    Dependencies: {pkg.requires}")
    except ImportError:
        print("Could not check installed packages.")

if __name__ == "__main__":
    main()