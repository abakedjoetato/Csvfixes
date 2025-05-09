"""
Fix SFTP Connect Return Type

This script fixes the SFTP client connect() method to return the client object 
rather than a boolean value, which was causing errors in the CSV processing system.
"""

import asyncio
import logging
import os
import re
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("fix_sftp_return.log", mode="w"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("sftp_fix")

# Path to the file that needs modification
SFTP_PATH = "utils/sftp.py"

async def create_backup():
    """Create a backup of the file to be modified"""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    
    if os.path.exists(SFTP_PATH):
        backup_path = f"{SFTP_PATH}.{timestamp}.backup"
        os.system(f"cp {SFTP_PATH} {backup_path}")
        logger.info(f"Created backup of {SFTP_PATH} at {backup_path}")

async def fix_sftp_connect_method():
    """Fix the connect method in SFTPClient class to return self instead of boolean"""
    try:
        # Create backup first
        await create_backup()
        
        # Read the SFTP utility file
        with open(SFTP_PATH, "r") as f:
            content = f.read()
        
        # Track changes
        issues_found = []
        fixes_applied = []
        
        # Check if the current method is returning True or False
        connect_method_pattern = r'async def connect\(self\)(.*?)return (True|False)'
        
        # Find connect method instances with re.DOTALL to match across lines
        connect_methods = re.finditer(connect_method_pattern, content, re.DOTALL)
        
        modified_content = content
        for match in connect_methods:
            method_text = match.group(0)
            return_value = match.group(2)  # The True/False part
            
            if return_value == 'True':
                # Replace 'return True' with 'return self'
                new_method_text = method_text.replace('return True', 'return self')
                modified_content = modified_content.replace(method_text, new_method_text)
                issues_found.append(f"Found 'return True' in connect() method that should return self")
                fixes_applied.append(f"Changed 'return True' to 'return self' in connect() method")
            
            if return_value == 'False':
                # Replace 'return False' with 'return self'  
                new_method_text = method_text.replace('return False', 'self._connected = False\n        return self')
                modified_content = modified_content.replace(method_text, new_method_text)
                issues_found.append(f"Found 'return False' in connect() method that should return self")
                fixes_applied.append(f"Changed 'return False' to 'self._connected = False; return self' in connect() method")
        
        # If we made changes, write them back
        if modified_content != content:
            with open(SFTP_PATH, "w") as f:
                f.write(modified_content)
            logger.info(f"Modified {SFTP_PATH} to fix connect() method return type")
            
            # Log issues and fixes
            for issue in issues_found:
                logger.info(f"Issue found: {issue}")
            for fix in fixes_applied:
                logger.info(f"Fix applied: {fix}")
            
            return True
        else:
            logger.info("No changes needed to SFTP connect method - already returning correct type")
            return False
            
    except Exception as e:
        logger.error(f"Error fixing SFTP connect method: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

async def run_fix():
    """Run the fix"""
    success = await fix_sftp_connect_method()
    
    if success:
        print("\n✅ SFTP connect() method successfully fixed to return client instance\n")
    else:
        print("\n⚠️ No changes were needed or an error occurred - check the logs\n")

if __name__ == "__main__":
    asyncio.run(run_fix())