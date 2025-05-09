"""
Command Handler Upgrade Tool

This script helps upgrade traditional command handlers to use the enhanced 
command_handler decorator for better error handling, validation, and metrics.

It analyzes existing command definitions, determines what features they need,
and suggests the appropriate command_handler decorator configuration.
"""
import os
import sys
import re
import ast
import astor
import importlib
import importlib.util
import logging
from typing import Dict, List, Set, Tuple, Any, Optional, Union
from pathlib import Path

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("command_upgrader")

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Common features to identify
FEATURES = {
    "premium": [
        r"premium",
        r"check_feature_access",
        r"has_feature_access",
        r"validate_premium_feature"
    ],
    "server_validation": [
        r"server_id",
        r"validate_server",
        r"standardize_server_id",
        r"get_server",
        r"check_server"
    ],
    "server_limit": [
        r"server_limit",
        r"check_server_limit",
        r"validate_server_limit",
        r"max_server"
    ],
    "guild_check": [
        r"ctx\.guild",
        r"interaction\.guild",
        r"if not ctx\.guild",
        r"if not interaction\.guild"
    ],
    "cooldown": [
        r"cooldown",
        r"rate_limit",
        r"wait",
        r"command\.cooldown",
        r"commands\.cooldown"
    ],
    "error_handling": [
        r"try\s*:",
        r"except",
        r"error_callback",
        r"on_error"
    ]
}

class CommandUpgrader(ast.NodeVisitor):
    """AST visitor to analyze and upgrade command functions."""
    
    def __init__(self, filename):
        self.filename = filename
        self.current_function = None
        self.command_functions = []
        self.command_features = {}
        self.current_features = set()
        self.server_id_params = set()
        
    def visit_FunctionDef(self, node):
        """Visit function definition."""
        # Check if this function is a command
        is_command = False
        old_decorators = []
        for decorator in node.decorator_list:
            decorator_text = astor.to_source(decorator).strip()
            if "command" in decorator_text and "command_handler" not in decorator_text:
                is_command = True
                old_decorators.append(decorator_text)
        
        if is_command:
            self.current_function = node.name
            self.command_functions.append(node.name)
            self.current_features = set()
            self.server_id_params = set()
            
            # Check function parameters
            for arg in node.args.args:
                if hasattr(arg, 'arg'):
                    arg_name = arg.arg
                    # Skip self, ctx, and interaction
                    if arg_name in ('self', 'ctx', 'interaction'):
                        continue
                    
                    # Check for server_id parameters
                    if 'server_id' in arg_name.lower() or ('server' in arg_name.lower() and 'id' in arg_name.lower()):
                        self.server_id_params.add(arg_name)
            
            # Visit the function body
            self.generic_visit(node)
            
            # Store features for this command
            self.command_features[node.name] = {
                "features": list(self.current_features),
                "server_id_params": list(self.server_id_params),
                "old_decorators": old_decorators
            }
            
            self.current_function = None
        else:
            self.generic_visit(node)
    
    def visit_Try(self, node):
        """Visit try block."""
        if self.current_function:
            self.current_features.add("error_handling")
        self.generic_visit(node)
    
    def visit_If(self, node):
        """Visit if statement."""
        if self.current_function:
            # Check for guild-only checks
            if_source = astor.to_source(node).strip()
            if any(re.search(pattern, if_source) for pattern in FEATURES["guild_check"]):
                self.current_features.add("guild_check")
        self.generic_visit(node)
    
    def visit_Call(self, node):
        """Visit function calls."""
        if self.current_function:
            # Convert call to source to check for various features
            call_source = astor.to_source(node).strip()
            
            # Check for all feature patterns
            for feature, patterns in FEATURES.items():
                if any(re.search(pattern, call_source) for pattern in patterns):
                    self.current_features.add(feature)
                
        self.generic_visit(node)


def find_command_files() -> List[str]:
    """Find all Python files that likely contain command definitions."""
    command_files = []
    
    # Directories to check
    directories = [
        ".",
        "./cogs",
        "./utils",
        "./commands"
    ]
    
    for directory in directories:
        if os.path.exists(directory):
            for file in os.listdir(directory):
                if file.endswith(".py"):
                    file_path = os.path.join(directory, file)
                    
                    # Check if file contains command patterns but not our enhanced handler
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                        
                        if "command" in content and "@command" in content and "command_handler" not in content:
                            command_files.append(file_path)
    
    return command_files


def analyze_command_file(file_path: str) -> Dict[str, Dict[str, Any]]:
    """
    Analyze a Python file for command definitions and what features they use.
    
    Args:
        file_path: Path to Python file
        
    Returns:
        Dict[str, Dict[str, Any]]: Command features
    """
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        # Parse the file
        tree = ast.parse(content)
        
        # Visit the AST
        upgrader = CommandUpgrader(file_path)
        upgrader.visit(tree)
        
        return upgrader.command_features
        
    except Exception as e:
        logger.error(f"Error analyzing {file_path}: {e}")
        return {}


def generate_command_handler_decorator(features: Dict[str, Any]) -> str:
    """
    Generate a command_handler decorator based on the features a command uses.
    
    Args:
        features: Features dictionary for a command
        
    Returns:
        str: Generated decorator
    """
    args = []
    
    # Handle premium features
    if "premium" in features["features"]:
        # Try to determine which premium feature is used
        # For now just use a placeholder
        args.append('premium_feature="advanced_stats"')
    
    # Handle server ID validation
    if "server_validation" in features["features"] and features["server_id_params"]:
        # Use the first server ID parameter
        args.append(f'server_id_param="{features["server_id_params"][0]}"')
    
    # Handle server limits
    if "server_limit" in features["features"]:
        args.append('check_server_limits=True')
    
    # Handle guild-only
    if "guild_check" in features["features"]:
        args.append('guild_only_command=True')
    else:
        args.append('guild_only_command=False')
    
    # Handle cooldowns
    if "cooldown" in features["features"]:
        # Default cooldown of 3 seconds
        args.append('cooldown_seconds=3')
    
    # Construct decorator
    if args:
        args_str = ", ".join(args)
        return f"@command_handler({args_str})"
    else:
        return "@command_handler()"


def suggest_command_upgrades() -> Dict[str, Any]:
    """
    Analyze codebase and suggest command handler upgrades.
    
    Returns:
        Dict[str, Any]: Upgrade suggestions
    """
    # Find command files
    command_files = find_command_files()
    logger.info(f"Found {len(command_files)} files with potential commands to upgrade")
    
    # Analyze each file
    all_upgrades = {}
    file_upgrades = {}
    
    for file_path in command_files:
        command_features = analyze_command_file(file_path)
        
        if command_features:
            # Generate upgrade suggestions for each command
            file_suggestions = {}
            for cmd_name, features in command_features.items():
                new_decorator = generate_command_handler_decorator(features)
                file_suggestions[cmd_name] = {
                    "old_decorators": features["old_decorators"],
                    "new_decorator": new_decorator,
                    "features_used": features["features"]
                }
            
            all_upgrades.update(file_suggestions)
            file_upgrades[file_path] = file_suggestions
            logger.info(f"Generated upgrade suggestions for {len(file_suggestions)} commands in {file_path}")
    
    # Generate report
    result = {
        "command_files": command_files,
        "upgrade_count": len(all_upgrades),
        "upgrades_by_file": file_upgrades,
        "all_upgrades": all_upgrades
    }
    
    return result


def generate_upgrade_report(results: Dict[str, Any]) -> str:
    """
    Generate a human-readable report of upgrade suggestions.
    
    Args:
        results: Upgrade suggestion results
        
    Returns:
        str: Formatted report
    """
    lines = [
        "# Command Handler Upgrade Report",
        f"Files Analyzed: {len(results['command_files'])}",
        f"Commands to Upgrade: {results['upgrade_count']}",
        "",
        "This report suggests upgrading traditional command decorators to use the enhanced `command_handler` decorator.",
        "The enhanced decorator provides better error handling, parameter validation, and command metrics.",
        ""
    ]
    
    # Group suggestions by file
    if results['upgrades_by_file']:
        lines.append("## Upgrade Suggestions by File")
        
        for file_path, suggestions in sorted(results['upgrades_by_file'].items()):
            lines.append(f"### {file_path}")
            lines.append("```python")
            lines.append("# Add this import at the top of the file:")
            lines.append("from utils.decorators import command_handler")
            lines.append("```")
            
            for cmd_name, upgrade in suggestions.items():
                lines.append(f"#### Command: `{cmd_name}`")
                lines.append("Replace:")
                lines.append("```python")
                for old_decorator in upgrade["old_decorators"]:
                    lines.append(old_decorator)
                lines.append(f"async def {cmd_name}(...):") 
                lines.append("```")
                
                lines.append("With:")
                lines.append("```python")
                lines.append(upgrade["new_decorator"])
                lines.append(f"async def {cmd_name}(...):") 
                lines.append("```")
                
                lines.append("Features used:")
                for feature in upgrade["features_used"]:
                    lines.append(f"- {feature}")
                lines.append("")
            
            lines.append("")
    
    # Add implementation notes
    lines.append("## Implementation Notes")
    lines.append("1. The `command_handler` decorator automatically handles:")
    lines.append("   - Error tracking and logging")
    lines.append("   - Guild validation")
    lines.append("   - Server ID validation")
    lines.append("   - Premium feature access checks")
    lines.append("   - Command metrics collection")
    lines.append("   - Rate limiting and cooldowns")
    lines.append("")
    lines.append("2. When upgrading commands, you may remove:")
    lines.append("   - Manual try/except blocks for common errors")
    lines.append("   - Manual server ID validation code")
    lines.append("   - Manual guild-only checks")
    lines.append("   - Manual premium feature checks")
    lines.append("")
    lines.append("3. You'll need to adjust the premium feature names to match the actual features used.")
    
    return "\n".join(lines)


def main():
    """Generate command handler upgrade suggestions."""
    logger.info("Analyzing commands for potential upgrades...")
    
    # Generate suggestions
    results = suggest_command_upgrades()
    
    # Generate report
    report = generate_upgrade_report(results)
    
    # Print report
    print(report)
    
    # Save to file
    with open("command_upgrade_report.md", "w") as f:
        f.write(report)
    
    logger.info(f"Analysis complete. Found {results['upgrade_count']} commands that could be upgraded.")
    return results


if __name__ == "__main__":
    # Run the analysis
    try:
        results = main()
    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()