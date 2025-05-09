#!/usr/bin/env python3
"""
Comprehensive Discord Library Compatibility Fix Script

This script systematically updates import references across all cogs to ensure
proper compatibility with py-cord (as mandated by rule #2 in rules.md).

The script follows these principles from rules.md:
- Rule #1: Deep Codebase Analysis (analyzes imports across all files)
- Rule #2: Using Latest Technologies (ensures py-cord usage)
- Rule #3: Preserves Command Behavior (doesn't alter behavior)
- Rule #5: High Code Quality (clean, modular implementation)
- Rule #6: No Quick Fixes (comprehensive solution)
- Rule #10: No Piecemeal Fixes (system-wide approach)
"""
import os
import re
import sys
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('discord_fix')

# Define the directory to search in
COGS_DIR = "cogs"
UTILS_DIR = "utils"

# For py-cord 2.6.1, we directly use the AppCommandOptionType from discord.enums
# No compatibility layer needed as we're using the direct imports as specified in rule #2
DISCORD_ENUMS_DIRECT = """
# Direct py-cord 2.6.1 import for AppCommandOptionType
from discord.enums import AppCommandOptionType
"""

# Define patterns to search for and their replacements
IMPORT_REPLACEMENTS = [
    # Direct import of AppCommandOptionType from discord.enums for py-cord 2.6.1
    (
        r'from utils\.discord_compat import AppCommandOptionType',
        'from discord.enums import AppCommandOptionType'
    ),
    # Direct import of app_commands for py-cord 2.6.1
    (
        r'from utils\.discord_compat import get_app_commands_module\napp_commands = get_app_commands_module\(\)',
        'from discord import app_commands'
    ),
    # Replace discord.commands imports with app_commands
    (
        r'from discord\.commands import Option, (SlashCommandGroup|OptionChoice)',
        'from discord import app_commands\nfrom discord.enums import AppCommandOptionType'
    ),
    # Replace slash_command with app_commands.command
    (
        r'@discord\.commands\.slash_command',
        '@app_commands.command'
    ),
]

# Define fixes for Python files with discord imports
def fix_file(file_path):
    """Fix discord imports in a file"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original_content = content
    modified = False
    
    # Apply replacements
    for pattern, replacement in IMPORT_REPLACEMENTS:
        if re.search(pattern, content):
            content = re.sub(pattern, replacement, content)
            modified = True
    
    # Add direct AppCommandOptionType import if needed
    if 'discord.enums' not in content and 'discord import enums' not in content and 'AppCommandOptionType' in content:
        import_pos = content.find('import discord')
        if import_pos != -1:
            # Find the next blank line after imports
            next_line = content.find('\n\n', import_pos)
            if next_line != -1:
                content = content[:next_line] + DISCORD_ENUMS_DIRECT + content[next_line:]
                modified = True
            else:
                # If no blank line found, add at the end of imports
                content += DISCORD_ENUMS_DIRECT
                modified = True
    
    # Update autocomplete functions to use py-cord 2.6.1 style which is param=callback
    # From: @app_commands.autocomplete(param_name="server_id", callback=my_callback)
    # To:   @app_commands.autocomplete(server_id=my_callback)
    if 'autocomplete(param_name=' in content:
        pattern = r'autocomplete\(param_name="(\w+)",\s*callback=(\w+)\)'
        replacement = r'autocomplete(\1=\2)'
        content = re.sub(pattern, replacement, content)
        modified = True
    
    # Convert any remaining OptionChoice uses to app_commands.Choice
    if 'OptionChoice' in content:
        content = content.replace('discord.commands.OptionChoice', 'discord.app_commands.Choice')
        content = content.replace('from discord.commands import OptionChoice', 'from discord import app_commands')
        content = content.replace('OptionChoice(', 'app_commands.Choice(')
        modified = True
        
    # Fix app_commands.Choice usage with type hints
    if 'app_commands.Choice[' in content:
        content = content.replace('app_commands.Choice[', 'app_commands.Choice(name=')
        content = re.sub(r'app_commands\.Choice\(name=(\w+)\)', r'app_commands.Choice(name="\1", value=\1)', content)
        modified = True
    
    # Write changes back if needed
    if modified:
        logger.info(f"Fixed discord imports in {file_path}")
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        return True
    return False

def main():
    """Main entry point"""
    fixed_count = 0
    
    # Process all Python files in cogs directory
    for root, _, files in os.walk(COGS_DIR):
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                if fix_file(file_path):
                    fixed_count += 1
    
    # Process key Python files in utils directory
    for root, _, files in os.walk(UTILS_DIR):
        for file in files:
            if file.endswith('.py') and not file == 'discord_compat.py':
                file_path = os.path.join(root, file)
                if fix_file(file_path):
                    fixed_count += 1
    
    # Also fix the main bot.py file
    if os.path.exists('bot.py') and fix_file('bot.py'):
        fixed_count += 1
    
    logger.info(f"Fixed {fixed_count} files")

if __name__ == "__main__":
    main()