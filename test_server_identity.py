#!/usr/bin/env python3
"""
Test script for server_identity module.
This verifies our identification system works correctly across UUID changes.
"""

import logging
import sys
from pprint import pprint

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Test the server_identity module with various server IDs and parameters"""
    try:
        # Import the server_identity module
        from utils.server_identity import identify_server, get_path_components
        
        # Test cases for server identification
        test_cases = [
            # Tower of Temptation server with original UUID
            {
                "name": "Tower of Temptation (original UUID)",
                "server_id": "1b1ab57e-8749-4a40-b7a1-b1073a5f24b3",
                "hostname": "79.127.236.1",
                "server_name": "Tower of Temptation",
                "guild_id": "1089234567890123456"
            },
            # Tower of Temptation server with new UUID after reset
            {
                "name": "Tower of Temptation (new UUID)",
                "server_id": "1056852d-05f9-4e5e-9e88-012c2870c042",
                "hostname": "79.127.236.1",
                "server_name": "Tower of Temptation",
                "guild_id": "1089234567890123456"
            },
            # Tower of Temptation server with hypothetical future UUID
            {
                "name": "Tower of Temptation (future UUID)",
                "server_id": "abcdef12-3456-7890-abcd-ef1234567890",
                "hostname": "79.127.236.1",
                "server_name": "Tower of Temptation",
                "guild_id": "1089234567890123456"
            },
            # Random server with UUID containing numeric IDs
            {
                "name": "Server with numeric ID in UUID",
                "server_id": "1234abcd-5678-90ef-1234-567890abcdef",
                "hostname": "192.168.1.100",
                "server_name": "Test Server",
                "guild_id": "2089234567890123456"
            },
            # Server with numeric ID
            {
                "name": "Server with numeric ID",
                "server_id": "9876",
                "hostname": "192.168.1.200",
                "server_name": "Server 9876",
                "guild_id": "3089234567890123456"
            },
            # Server with hostname containing ID
            {
                "name": "Server with hostname ID",
                "server_id": "abcdef12-3456-7890-abcd-ef1234567890",
                "hostname": "gameserver.com_5432",
                "server_name": "Game Server",
                "guild_id": "4089234567890123456"
            },
            # Server with name containing ID
            {
                "name": "Server with name ID",
                "server_id": "abcdef12-3456-7890-abcd-ef1234567890",
                "hostname": "gameserver.com",
                "server_name": "Game Server 7890",
                "guild_id": "5089234567890123456"
            }
        ]
        
        # Process each test case
        results = []
        for case in test_cases:
            # Call identify_server
            numeric_id, is_known = identify_server(
                server_id=case["server_id"],
                hostname=case["hostname"],
                server_name=case["server_name"],
                guild_id=case["guild_id"]
            )
            
            # Call get_path_components
            server_dir, path_server_id = get_path_components(
                server_id=case["server_id"],
                hostname=case["hostname"],
                guild_id=case["guild_id"]
            )
            
            # Store results
            result = {
                "name": case["name"],
                "server_id": case["server_id"],
                "result": {
                    "numeric_id": numeric_id,
                    "is_known": is_known,
                    "server_dir": server_dir,
                    "path_server_id": path_server_id
                }
            }
            results.append(result)
        
        # Print results
        print("\n===== SERVER IDENTITY TEST RESULTS =====")
        for result in results:
            print(f"\nTest Case: {result['name']}")
            print(f"  Original server_id: {result['server_id']}")
            print(f"  Identified numeric_id: {result['result']['numeric_id']}")
            print(f"  Is known server: {result['result']['is_known']}")
            print(f"  Server directory: {result['result']['server_dir']}")
            print(f"  Path server ID: {result['result']['path_server_id']}")
        
        print("\n===== TESTING MULTIPLE TOWER OF TEMPTATION UUID VARIATIONS =====")
        # Test multiple Tower of Temptation UUIDs to show they all resolve to the same ID
        tot_uuids = [
            "1b1ab57e-8749-4a40-b7a1-b1073a5f24b3",  # Original
            "1056852d-05f9-4e5e-9e88-012c2870c042",  # Current
            "abc123de-f456-7890-abcd-ef1234567890",  # Hypothetical future
            "xyz987ab-cdef-1234-5678-90abcdef1234"   # Another hypothetical future
        ]
        
        for uuid in tot_uuids:
            numeric_id, is_known = identify_server(
                server_id=uuid,
                hostname="79.127.236.1",
                server_name="Tower of Temptation",
                guild_id="1089234567890123456"
            )
            print(f"UUID: {uuid} -> numeric_id: {numeric_id}, is_known: {is_known}")
        
        print("\n===== TESTING SERVER ID PERSISTENCE ACROSS PARAMETERS =====")
        # Test detection from name only
        name_only_id, name_only_known = identify_server(
            server_id="unknown-uuid-12345",
            hostname="",
            server_name="Tower of Temptation Server",
            guild_id=""
        )
        print(f"From name only: numeric_id: {name_only_id}, is_known: {name_only_known}")
        
        # Test detection from hostname only
        hostname_with_tot = "tower.of.temptation.game.server"
        hostname_only_id, hostname_only_known = identify_server(
            server_id="unknown-uuid-67890",
            hostname=hostname_with_tot,
            server_name="",
            guild_id=""
        )
        print(f"From hostname only: numeric_id: {hostname_only_id}, is_known: {hostname_only_known}")
        
        # Test detection from IP address only
        hostname_with_ip = "79.127.236.1"
        ip_only_id, ip_only_known = identify_server(
            server_id="randomuuid-1234-5678-9012-345678901234",
            hostname=hostname_with_ip,
            server_name="Some Random Server",
            guild_id=""
        )
        print(f"From IP address only: numeric_id: {ip_only_id}, is_known: {ip_only_known}")
        
        return 0
    except Exception as e:
        logger.error(f"Error in test: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

if __name__ == "__main__":
    sys.exit(main())