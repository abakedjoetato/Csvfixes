"""
Forced clean reinstallation of py-cord
"""
import subprocess
import os
import sys
import shutil
import time

def run_command(cmd):
    """Run a command and return output"""
    process = subprocess.run(cmd, shell=True, 
                           stdout=subprocess.PIPE, 
                           stderr=subprocess.PIPE,
                           text=True)
    return process.returncode, process.stdout, process.stderr

def main():
    print("=== Starting Forced Reinstallation of py-cord ===")
    
    # 1. First completely uninstall all discord packages
    print("\n1. Removing all discord packages...")
    _, out, err = run_command(f"{sys.executable} -m pip uninstall -y discord discord.py py-cord discord-py")
    print(out)
    if err:
        print(f"Errors: {err}")
    
    # 2. Check where the site packages directory is
    print("\n2. Locating site-packages directory...")
    import site
    site_packages = site.getsitepackages()[0]
    print(f"Site packages directory: {site_packages}")
    
    # 3. Manually delete any remaining discord directories
    print("\n3. Manually removing any remaining discord directories...")
    discord_paths = [
        os.path.join(site_packages, 'discord'),
        os.path.join(site_packages, 'discord.py'),
        os.path.join(site_packages, 'discord.py-*'),
        os.path.join(site_packages, 'py_cord'),
        os.path.join(site_packages, 'py_cord-*'),
    ]
    
    for path in discord_paths:
        if os.path.exists(path):
            print(f"Removing {path}")
            try:
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
            except Exception as e:
                print(f"Error removing {path}: {e}")
    
    # 4. Install py-cord with specific version
    print("\n4. Installing py-cord 2.6.1...")
    print("First, installing with --no-deps...")
    _, out, err = run_command(f"{sys.executable} -m pip install --no-deps py-cord==2.6.1")
    print(out)
    if err:
        print(f"Errors: {err}")
    
    print("\nInstalling dependencies...")
    deps = ["aiohttp>=3.7.4"]
    for dep in deps:
        _, out, err = run_command(f"{sys.executable} -m pip install {dep}")
        print(f"Installed {dep}: {out}")
    
    # 5. Verify py-cord is correctly installed
    print("\n5. Verifying installation...")
    try:
        print("Trying to import discord...")
        import importlib
        import importlib.metadata
        importlib.invalidate_caches()  # Clear import caches
        
        discord = importlib.import_module("discord")
        print(f"Successfully imported discord, version: {getattr(discord, '__version__', 'unknown')}")
        
        # Check if it's py-cord by checking bridge module
        bridge_exists = importlib.util.find_spec("discord.ext.bridge") is not None
        print(f"Bridge module exists: {bridge_exists}")
        
        # Check py-cord specifics
        slash_cmd_exists = False
        try:
            cmd_module = importlib.import_module("discord.ext.commands")
            slash_cmd_exists = hasattr(cmd_module, 'slash_command')
        except:
            pass
        print(f"Has slash_command: {slash_cmd_exists}")
        
        if bridge_exists or slash_cmd_exists:
            print("✅ SUCCESS: py-cord appears to be correctly installed")
            return 0
        else:
            print("❌ FAILURE: py-cord is not correctly installed")
            return 1
    except Exception as e:
        print(f"Error verifying installation: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())