"""
Test script to understand the import structure of py-cord 2.6.1

This script attempts to import various modules from discord namespace
to determine which ones are available and which ones are from py-cord vs discord.py
"""

import sys
import importlib
import inspect

def check_version(obj):
    """Check the version of an object by inspecting its module"""
    try:
        module = inspect.getmodule(obj)
        if hasattr(module, '__version__'):
            return module.__version__
        # Try to get the top-level module
        top_module = sys.modules.get(module.__name__.split('.')[0])
        if top_module and hasattr(top_module, '__version__'):
            return top_module.__version__
        return "Unknown"
    except Exception as e:
        return f"Error: {e}"

def test_imports():
    """Test various import structures and report on their availability"""
    imports_to_check = [
        "discord",
        "discord.ext",
        "discord.ext.commands",
        "discord.app_commands",
        "discord.abc",
        "discord.ui",
        "discord.utils",
        "discord.errors",
        "discord.guild",
        "discord.gateway",
        "discord.voice_client"
    ]
    
    results = {}
    for module_name in imports_to_check:
        try:
            module = importlib.import_module(module_name)
            results[module_name] = {
                "available": True,
                "version": getattr(module, "__version__", "Unknown"),
                "path": getattr(module, "__file__", "Unknown"),
            }
        except ImportError as e:
            results[module_name] = {
                "available": False,
                "error": str(e)
            }
    
    # Print results
    print("Import test results:")
    print("=" * 60)
    for name, result in results.items():
        if result["available"]:
            print(f"✓ {name}: Version {result['version']}, Path: {result['path']}")
        else:
            print(f"✗ {name}: {result['error']}")
    
def test_classes():
    """Test importing various classes from the discord namespace"""
    classes_to_check = [
        ("discord", "Client"),
        ("discord", "Bot"),
        ("discord", "AutoShardedBot"),
        ("discord", "Intents"),
        ("discord.ext.commands", "Bot"),
        ("discord.ext.commands", "Command"),
        ("discord.ext.commands", "Context"),
        ("discord.app_commands", "Command"),
        ("discord.app_commands", "CommandTree"),
        ("discord.ui", "View"),
        ("discord.ui", "Button"),
        ("discord.ui", "Select"),
    ]
    
    results = {}
    for module_name, class_name in classes_to_check:
        try:
            module = importlib.import_module(module_name)
            class_obj = getattr(module, class_name, None)
            if class_obj is not None:
                results[f"{module_name}.{class_name}"] = {
                    "available": True,
                    "version": check_version(class_obj),
                }
            else:
                results[f"{module_name}.{class_name}"] = {
                    "available": False,
                    "error": f"Class {class_name} not found in module {module_name}"
                }
        except ImportError as e:
            results[f"{module_name}.{class_name}"] = {
                "available": False,
                "error": str(e)
            }
        except AttributeError as e:
            results[f"{module_name}.{class_name}"] = {
                "available": False,
                "error": str(e)
            }
    
    # Print results
    print("\nClass import test results:")
    print("=" * 60)
    for name, result in results.items():
        if result["available"]:
            print(f"✓ {name}: Version {result['version']}")
        else:
            print(f"✗ {name}: {result['error']}")

if __name__ == "__main__":
    print(f"Python version: {sys.version}")
    test_imports()
    test_classes()