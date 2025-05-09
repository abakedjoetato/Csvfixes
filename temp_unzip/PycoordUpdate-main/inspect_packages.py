#!/usr/bin/env python3
"""
Script to inspect package relationships and import paths

This script helps understand why py-cord and discord.py might be
conflicting by examining their package structures and import hooks.
"""

import sys
import inspect
import os
import pkgutil
import importlib
import importlib.metadata
from pprint import pprint

def inspect_package_metadata(package_name):
    """Inspect package metadata using importlib.metadata"""
    print(f"\n=== Inspecting metadata for {package_name} ===")
    try:
        # Get distributions with the specified name
        dists = list(importlib.metadata.distributions())
        matching_dists = [d for d in dists if d.metadata.get('Name', '').lower() == package_name.lower()]
        
        if not matching_dists:
            print(f"No distribution found for {package_name}")
            return
        
        for dist in matching_dists:
            print(f"Distribution: {dist.metadata.get('Name')} {dist.metadata.get('Version')}")
            
            # Get package dependencies
            try:
                deps = dist.requires or []
                if deps:
                    print("Dependencies:")
                    for dep in deps:
                        print(f"  - {dep}")
                else:
                    print("No dependencies listed")
            except Exception as e:
                print(f"Error getting dependencies: {e}")
            
            # Get package entry points
            try:
                entry_points = dist.entry_points
                if entry_points:
                    print("Entry points:")
                    for ep in entry_points:
                        print(f"  - {ep.name} = {ep.value}")
                else:
                    print("No entry points")
            except Exception as e:
                print(f"Error getting entry points: {e}")
            
            # Get top-level modules
            try:
                if hasattr(dist, "files"):
                    top_level_modules = set()
                    for file in dist.files:
                        if file.name.endswith('.py'):
                            parts = file.name.split('/')
                            if len(parts) == 1:
                                top_level_modules.add(parts[0])
                            elif len(parts) > 1:
                                top_level_modules.add(parts[0])
                    
                    if top_level_modules:
                        print("Top-level modules:")
                        for module in sorted(top_level_modules):
                            print(f"  - {module}")
                    else:
                        print("No top-level modules found")
            except Exception as e:
                print(f"Error getting top-level modules: {e}")
    
    except Exception as e:
        print(f"Error inspecting metadata: {e}")

def inspect_module_details(module_name):
    """Inspect details of a module including path and exports"""
    print(f"\n=== Inspecting module {module_name} ===")
    try:
        module = importlib.import_module(module_name)
        
        # Check module path
        if hasattr(module, '__file__'):
            print(f"Module path: {module.__file__}")
        else:
            print("Module has no __file__ attribute")
        
        # Check package path
        if hasattr(module, '__path__'):
            print(f"Package path: {module.__path__}")
        else:
            print("Module has no __path__ attribute")
        
        # Check version
        if hasattr(module, '__version__'):
            print(f"Version: {module.__version__}")
        else:
            print("Module has no __version__ attribute")
        
        # Inspect submodules if it's a package
        if hasattr(module, '__path__'):
            print(f"\nSubmodules of {module_name}:")
            for finder, name, is_pkg in pkgutil.iter_modules(module.__path__, module.__name__ + '.'):
                print(f"  {'[pkg]' if is_pkg else '[mod]'} {name}")
        
        # List some key exports
        print(f"\nKey exports from {module_name}:")
        important_types = [
            'Intents', 'Client', 'Bot', 'AutoShardedBot', 'AutoShardedClient',
            'app_commands', 'SlashCommand', 'ApplicationCommand', 'SlashCommandGroup',
            'Interaction', 'Command', 'CommandError'
        ]
        
        found_exports = []
        for name in dir(module):
            if name in important_types or name.startswith('__') and name.endswith('__'):
                obj = getattr(module, name)
                obj_type = type(obj).__name__
                found_exports.append((name, obj_type))
        
        for name, obj_type in sorted(found_exports):
            print(f"  - {name}: {obj_type}")
            
    except ImportError as e:
        print(f"Cannot import {module_name}: {e}")
    except Exception as e:
        print(f"Error inspecting module {module_name}: {e}")

def check_import_conflicts():
    """Check for import conflicts between py-cord and discord.py"""
    print("\n=== Checking for import conflicts ===")
    
    # Try importing specific modules that might conflict
    conflict_checks = [
        ('discord', 'Package import'),
        ('discord.ext', 'Extension package'),
        ('discord.ext.commands', 'Commands extension'),
        ('discord.app_commands', 'Application commands')
    ]
    
    for module_path, description in conflict_checks:
        try:
            module = importlib.import_module(module_path)
            origin = getattr(module, '__file__', 'Unknown location')
            print(f"✓ {description} ({module_path}): {origin}")
        except ImportError as e:
            print(f"✗ {description} ({module_path}): {e}")
        except Exception as e:
            print(f"? {description} ({module_path}): Unknown error: {e}")

def main():
    """Main function"""
    print("Inspecting Python package environment...")
    print(f"Python version: {sys.version}")
    print(f"Python executable: {sys.executable}")
    print(f"Module search paths:")
    for path in sys.path:
        print(f"  - {path}")
    
    # Inspect package metadata
    inspect_package_metadata('py-cord')
    inspect_package_metadata('discord.py')
    inspect_package_metadata('discord-py')
    
    # Inspect module details
    inspect_module_details('discord')
    
    # Check for import conflicts
    check_import_conflicts()

if __name__ == "__main__":
    main()