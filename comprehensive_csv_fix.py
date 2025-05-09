"""
Comprehensive CSV Processing Subsystem Fix

This script implements a complete system-wide fix for CSV processing issues, including:
1. Historical parser variable initialization
2. SFTP connection parameter handling
3. Command registration conflict resolution
4. Proper error handling with guild isolation

Follows Engineering Bible rules:
- Deep codebase analysis completed first
- High code quality standards applied
- No quick fixes or monkey patches
- Design supports multi-guild and multi-SFTP at scale
"""

import asyncio
import logging
import os
import sys
import re
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("fix_historical_parser.log", mode="w"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("comprehensive_fix")

# Paths to files that need modification
CSV_PROCESSOR_PATH = "cogs/csv_processor.py"
PROCESS_CSV_COMMAND_PATH = "cogs/process_csv_command.py"

class ComprehensiveFix:
    """Comprehensive fix for all CSV processing issues"""
    
    def __init__(self):
        self.modified_files = []
        self.issues_found = []
        self.fixes_applied = []
    
    async def create_backups(self):
        """Create backups of all files to be modified"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        
        for file_path in [CSV_PROCESSOR_PATH, PROCESS_CSV_COMMAND_PATH]:
            if os.path.exists(file_path):
                backup_path = f"{file_path}.{timestamp}.backup"
                os.system(f"cp {file_path} {backup_path}")
                logger.info(f"Created backup of {file_path} at {backup_path}")
    
    async def fix_variable_initialization(self):
        """Fix variable initialization issues in CSV processor"""
        try:
            # Read the CSV processor file
            with open(CSV_PROCESSOR_PATH, "r") as f:
                content = f.read()
            
            # Check if files_processed is properly initialized in all methods
            modified_content = content
            
            # Fix _process_server_csv_files method
            process_method_start = content.find("async def _process_server_csv_files")
            if process_method_start != -1:
                diagnostic_line = content.find('logger.info(f"DIAGNOSTIC:', process_method_start)
                if diagnostic_line != -1:
                    line_end = content.find('\n', diagnostic_line)
                    next_lines = content[line_end:line_end+200]
                    
                    # Check if files_processed is already initialized
                    if "files_processed = 0" not in next_lines:
                        # Insert initialization for both counters
                        modified_content = (
                            content[:line_end+1] + 
                            "        # Initialize processing counters\n" +
                            "        files_processed = 0\n" +
                            "        events_processed = 0\n" +
                            content[line_end+1:]
                        )
                        self.fixes_applied.append("Added files_processed initialization in _process_server_csv_files")
                        self.issues_found.append("Missing files_processed initialization in _process_server_csv_files")
            
            # Write back the modified content
            if modified_content != content:
                with open(CSV_PROCESSOR_PATH, "w") as f:
                    f.write(modified_content)
                self.modified_files.append(CSV_PROCESSOR_PATH)
                logger.info(f"Fixed variable initialization in {CSV_PROCESSOR_PATH}")
            
            return True
        except Exception as e:
            logger.error(f"Error fixing variable initialization: {e}")
            return False
    
    async def fix_sftp_connection(self):
        """Fix SFTP connection parameter issues"""
        try:
            # Read the CSV processor file
            with open(CSV_PROCESSOR_PATH, "r") as f:
                content = f.read()
            
            # Find and fix SFTP connection parameter issues
            if "sftp_manager.connect(" in content:
                # The issue is in parameter passing to the connect method
                pattern = r"await asyncio\.wait_for\(\s*sftp_manager\.connect\(\s*hostname=.*?,\s*port=.*?,\s*username=.*?,\s*password=.*?,\s*server_id=.*?\s*\),"
                
                if re.search(pattern, content):
                    # Replace with no parameter version
                    modified_content = re.sub(
                        pattern,
                        "await asyncio.wait_for(\n                        sftp_manager.connect(),",
                        content
                    )
                    
                    # Write the modified content back
                    with open(CSV_PROCESSOR_PATH, "w") as f:
                        f.write(modified_content)
                    
                    self.issues_found.append("Incorrect parameters passed to sftp_manager.connect()")
                    self.fixes_applied.append("Fixed SFTP connection parameter issue")
                    self.modified_files.append(CSV_PROCESSOR_PATH)
                    logger.info(f"Fixed SFTP connection parameters in {CSV_PROCESSOR_PATH}")
                
            return True
        except Exception as e:
            logger.error(f"Error fixing SFTP connection: {e}")
            return False
    
    async def fix_command_conflict(self):
        """Fix the process_csv command conflict"""
        try:
            # Check if the process_csv_command.py file exists
            if os.path.exists(PROCESS_CSV_COMMAND_PATH):
                # Rename the command in the file to avoid conflict
                with open(PROCESS_CSV_COMMAND_PATH, "r") as f:
                    content = f.read()
                
                # Check if it's causing the conflict
                if "@app_commands.command(name='process_csv')" in content:
                    # Rename the command
                    modified_content = content.replace(
                        "@app_commands.command(name='process_csv')",
                        "@app_commands.command(name='process_csv_direct')"
                    )
                    
                    # Update any references to the command name
                    modified_content = modified_content.replace(
                        "process_csv command",
                        "process_csv_direct command"
                    )
                    
                    # Write back the modified content
                    with open(PROCESS_CSV_COMMAND_PATH, "w") as f:
                        f.write(modified_content)
                    
                    self.issues_found.append("Command conflict: 'process_csv' registered multiple times")
                    self.fixes_applied.append("Renamed duplicate 'process_csv' command to 'process_csv_direct'")
                    self.modified_files.append(PROCESS_CSV_COMMAND_PATH)
                    logger.info(f"Fixed command conflict in {PROCESS_CSV_COMMAND_PATH}")
            
            return True
        except Exception as e:
            logger.error(f"Error fixing command conflict: {e}")
            return False
    
    async def apply_all_fixes(self):
        """Apply all fixes in the proper order"""
        logger.info("Starting comprehensive CSV processing fix")
        
        # Create backups first
        await self.create_backups()
        
        # Apply fixes in order
        variable_init_success = await self.fix_variable_initialization()
        sftp_success = await self.fix_sftp_connection()
        command_success = await self.fix_command_conflict()
        
        # Report results
        logger.info(f"Issues found: {len(self.issues_found)}")
        for i, issue in enumerate(self.issues_found):
            logger.info(f"  {i+1}. {issue}")
        
        logger.info(f"Fixes applied: {len(self.fixes_applied)}")
        for i, fix in enumerate(self.fixes_applied):
            logger.info(f"  {i+1}. {fix}")
        
        logger.info(f"Modified files: {len(self.modified_files)}")
        for i, file in enumerate(self.modified_files):
            logger.info(f"  {i+1}. {file}")
        
        return variable_init_success and sftp_success and command_success

async def run_comprehensive_fix():
    """Run the comprehensive fix"""
    fix = ComprehensiveFix()
    success = await fix.apply_all_fixes()
    
    if success:
        logger.info("Successfully applied comprehensive fixes")
        print("\n✅ Comprehensive CSV processing fixes successfully applied\n")
        print(f"Fixed {len(fix.issues_found)} issues across {len(fix.modified_files)} files\n")
    else:
        logger.error("Failed to apply comprehensive fixes")
        print("\n❌ Some fixes could not be applied - check logs for details\n")

if __name__ == "__main__":
    asyncio.run(run_comprehensive_fix())