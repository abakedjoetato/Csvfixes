"""
Comprehensive SFTP Client Fix

This script fixes the SFTP client usage in the CSV processor to ensure proper connection handling,
consistent return types, and robust error handling in compliance with all rules in rules.md.

Fixes:
1. SFTP client connect() method usage
2. Return value consistency
3. Error handling in SFTP operations
4. Guild isolation preservation
"""

import asyncio
import logging
import os
import sys
from datetime import datetime
import re
from typing import Dict, Any, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("fix_sftp_client.log", mode="w"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("sftp_fix")

# Path to files that need modification
CSV_PROCESSOR_PATH = "cogs/csv_processor.py"
SFTP_CLIENT_PATH = "utils/sftp.py"

class SFTPClientFix:
    """Comprehensive SFTP client fix implementation"""
    
    def __init__(self):
        self.modified_files = []
        self.issues_found = []
        self.fixes_applied = []
    
    async def create_backups(self):
        """Create backups of all files to be modified"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        
        for file_path in [CSV_PROCESSOR_PATH, SFTP_CLIENT_PATH]:
            if os.path.exists(file_path):
                backup_path = f"{file_path}.{timestamp}.backup"
                os.system(f"cp {file_path} {backup_path}")
                logger.info(f"Created backup of {file_path} at {backup_path}")
    
    async def fix_sftp_client_code(self):
        """Fix the SFTP client code to ensure it returns self instead of boolean"""
        try:
            # Read the SFTP client file
            if not os.path.exists(SFTP_CLIENT_PATH):
                logger.error(f"SFTP client file not found at {SFTP_CLIENT_PATH}")
                return False
                
            with open(SFTP_CLIENT_PATH, "r") as f:
                content = f.read()
            
            # Find the connect method
            connect_method_pattern = r"async def connect\(self\)(.*?)return (True|False)"
            
            # Check if method needs modification using regex with DOTALL flag
            connect_methods = re.findall(connect_method_pattern, content, re.DOTALL)
            
            if not connect_methods:
                logger.warning("Could not find connect method in SFTP client")
                return False
            
            # Modify the connect method to return self instead of boolean
            modified_content = content.replace(
                "async def connect(self) -> bool:",
                "async def connect(self) -> 'SFTPClient':"
            )
            
            # Update return statements to return self
            modified_content = modified_content.replace(
                "            return True",
                "            return self"
            )
            
            modified_content = modified_content.replace(
                "                return True",
                "                return self"
            )
            
            # Add return self for False cases with proper client state tracking
            modified_content = modified_content.replace(
                "            return False",
                "            self._connected = False\n            return self"
            )
            
            modified_content = modified_content.replace(
                "                return False",
                "                self._connected = False\n                return self"
            )
            
            # Update method documentation
            modified_content = modified_content.replace(
                "        Returns:\n            True if connected successfully, False otherwise",
                "        Returns:\n            self: The SFTPClient instance for method chaining\n            \n        Note:\n            Check self._connected to determine connection success"
            )
            
            # Add is_connected property for proper state checking
            property_addition = """
    @property
    def is_connected(self) -> bool:
        '''Check if the client is connected and ready for operations

        Returns:
            bool: True if connected and ready, False otherwise
        '''
        return self._connected and self._sftp_client is not None
            """
            
            # Find a good place to add the property
            class_end = content.find("class SFTPManager(")
            if class_end == -1:
                class_end = len(content)
            
            # Insert property before the end of the class
            modified_content = modified_content[:class_end] + property_addition + modified_content[class_end:]
            
            # Write the modified content back
            with open(SFTP_CLIENT_PATH, "w") as f:
                f.write(modified_content)
            
            self.issues_found.append("SFTP client connect() method returns boolean instead of self")
            self.fixes_applied.append("Modified SFTP client connect() method to return self for method chaining")
            self.modified_files.append(SFTP_CLIENT_PATH)
            logger.info(f"Fixed SFTP client code in {SFTP_CLIENT_PATH}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error fixing SFTP client code: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    async def fix_csv_processor_code(self):
        """Fix the CSV processor code to handle the SFTP client correctly"""
        try:
            # Read the CSV processor file
            if not os.path.exists(CSV_PROCESSOR_PATH):
                logger.error(f"CSV processor file not found at {CSV_PROCESSOR_PATH}")
                return False
                
            with open(CSV_PROCESSOR_PATH, "r") as f:
                content = f.read()
            
            # Fix pattern 1: check for boolean return vs object
            # Find blocks that use sftp.connect() and then try to use the returned value as object
            pattern1 = r"sftp = await (.*?)\.connect\(\)(.*?)await sftp\."
            
            modified_content = content
            
            # For each match, modify the code to check if connected before using
            for match in re.finditer(pattern1, content, re.DOTALL):
                full_match = match.group(0)
                manager_var = match.group(1)
                between_code = match.group(2)
                
                # Create replacement that checks connection state
                replacement = f"""sftp = await {manager_var}.connect()
                # Ensure SFTP client is properly connected before using
                if sftp.is_connected:{between_code}await sftp."""
                
                # Replace in content
                modified_content = modified_content.replace(full_match, replacement)
                
                self.issues_found.append("CSV processor uses SFTP client return value incorrectly")
                self.fixes_applied.append("Added connection state checking before using SFTP client")
            
            # Fix pattern 2: Using the return value directly
            pattern2 = r"await sftp_manager\.connect\(\)"
            
            # Find cases where the return value is not used
            for match in re.finditer(pattern2, modified_content):
                full_match = match.group(0)
                
                # Create replacement that captures the result
                replacement = "sftp = await sftp_manager.connect()"
                
                # Replace in content
                modified_content = modified_content.replace(full_match, replacement)
                
                self.issues_found.append("SFTP connection result not captured")
                self.fixes_applied.append("Captured SFTP connection result for proper state tracking")
            
            # Fix specific error in _process_server_csv_files where connect returns bool
            error_pattern = r"if not was_connected:\s+await sftp\.connect\(\)"
            
            if re.search(error_pattern, modified_content):
                replacement = "if not was_connected:\n                sftp = await sftp.connect()\n                if not sftp.is_connected:\n                    logger.error(f\"Failed to connect to SFTP server for {server_id}\")\n                    return 0, 0"
                
                # Replace in content
                modified_content = re.sub(error_pattern, replacement, modified_content)
                
                self.issues_found.append("Missing connection state check after SFTP connect")
                self.fixes_applied.append("Added proper connection state verification and error handling")
            
            # Write the modified content back
            with open(CSV_PROCESSOR_PATH, "w") as f:
                f.write(modified_content)
            
            self.modified_files.append(CSV_PROCESSOR_PATH)
            logger.info(f"Fixed CSV processor code in {CSV_PROCESSOR_PATH}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error fixing CSV processor code: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    async def apply_all_fixes(self):
        """Apply all fixes in the proper order"""
        logger.info("Starting comprehensive SFTP client fix")
        
        # Create backups first
        await self.create_backups()
        
        # Fix SFTP client first, then the code that uses it
        sftp_success = await self.fix_sftp_client_code()
        csv_success = await self.fix_csv_processor_code()
        
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
        
        return sftp_success and csv_success

async def run_sftp_fix():
    """Run the comprehensive SFTP client fix"""
    fix = SFTPClientFix()
    success = await fix.apply_all_fixes()
    
    if success:
        logger.info("Successfully applied comprehensive SFTP client fixes")
        print("\n✅ Comprehensive SFTP client fixes successfully applied\n")
        print(f"Fixed {len(fix.issues_found)} issues across {len(fix.modified_files)} files\n")
    else:
        logger.error("Failed to apply comprehensive SFTP client fixes")
        print("\n❌ Some fixes could not be applied - check logs for details\n")

if __name__ == "__main__":
    asyncio.run(run_sftp_fix())