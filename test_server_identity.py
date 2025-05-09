"""
Test script for server identity module

This script tests the server identity module to ensure it correctly identifies servers
even across UUID changes with the database-driven identity system.
"""
import os
import sys
import asyncio
import logging
from typing import Dict, Any, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("test_server_identity")

# Import server identity module
try:
    from utils.server_identity import identify_server, get_path_components, load_server_mappings
    logger.info("Successfully imported server_identity module")
except ImportError as e:
    logger.error(f"Error importing server_identity module: {e}")
    sys.exit(1)

# Import database utilities
try:
    from utils.database import initialize_db, get_db
    logger.info("Successfully imported database module")
except ImportError as e:
    logger.error(f"Error importing database module: {e}")
    sys.exit(1)

async def setup_test_data():
    """Set up test data in the database"""
    db = await get_db()
    logger.info("Setting up test data in database")
    
    # Create test servers with UUID and original_server_id mappings
    test_servers = [
        {
            "server_id": "1b1ab57e-8749-4a40-b7a1-b1073a5f24b3",
            "original_server_id": "7020",
            "name": "Emeralds Killfeed",
            "hostname": "emerald.game.com",
            "guild_id": "1219706687980568769"
        },
        {
            "server_id": "1056852d-05f9-4e5e-9e88-012c2870c042",
            "original_server_id": "7020",  # Same ID for multiple UUIDs
            "name": "Emeralds Killfeed Reset",
            "hostname": "emerald-reset.game.com",
            "guild_id": "1219706687980568769"
        },
        {
            "server_id": "2c3de68f-95a0-412b-b3c2-d09eab7f8643",
            "original_server_id": "8030",
            "name": "Test Server",
            "hostname": "test.game.com",
            "guild_id": "1219706687980568769"
        },
        {
            "server_id": "3d4ef79g-06b1-523c-c4d3-e10fab8g9754",
            "original_server_id": "8030",  # Same ID for multiple UUIDs
            "name": "Test Server Reset",
            "hostname": "test-reset.game.com",
            "guild_id": "1219706687980568769"
        }
    ]
    
    # Insert test servers
    for server in test_servers:
        await db.game_servers.update_one(
            {"server_id": server["server_id"]},
            {"$set": server},
            upsert=True
        )
    
    logger.info(f"Inserted {len(test_servers)} test servers")
    return test_servers

async def perform_tests(test_servers):
    """Run tests for server identity functionality"""
    db = await get_db()
    
    # Load server mappings from database
    mapping_count = await load_server_mappings(db)
    logger.info(f"Loaded {mapping_count} server mappings from database")
    
    # Test 1: Test original UUID to ID
    logger.info("Test 1: Original UUID to ID mapping")
    original_uuid = test_servers[0]["server_id"]
    server_id, is_known = identify_server(original_uuid)
    logger.info(f"Original UUID: {original_uuid} -> Server ID: {server_id}, Is Known: {is_known}")
    assert server_id == "7020", f"Expected 7020, got {server_id}"
    assert is_known, "Expected is_known to be True"
    
    # Test 2: Test new UUID to ID (should map to same ID)
    logger.info("Test 2: New UUID to ID mapping")
    new_uuid = test_servers[1]["server_id"]
    server_id, is_known = identify_server(new_uuid)
    logger.info(f"New UUID: {new_uuid} -> Server ID: {server_id}, Is Known: {is_known}")
    assert server_id == "7020", f"Expected 7020, got {server_id}"
    assert is_known, "Expected is_known to be True"
    
    # Test 3: Test path components with original UUID
    logger.info("Test 3: Path components with original UUID")
    server_dir, path_id = get_path_components(
        original_uuid, 
        test_servers[0]["hostname"]
    )
    logger.info(f"Original UUID Path: {server_dir}, Path ID: {path_id}")
    assert path_id == "7020", f"Expected path_id 7020, got {path_id}"
    assert server_dir == "emerald.game.com_7020", f"Expected emerald.game.com_7020, got {server_dir}"
    
    # Test 4: Test path components with new UUID
    logger.info("Test 4: Path components with new UUID")
    server_dir, path_id = get_path_components(
        new_uuid, 
        test_servers[1]["hostname"]
    )
    logger.info(f"New UUID Path: {server_dir}, Path ID: {path_id}")
    assert path_id == "7020", f"Expected path_id 7020, got {path_id}"
    assert server_dir == "emerald-reset.game.com_7020", f"Expected emerald-reset.game.com_7020, got {server_dir}"
    
    # Test 5: Test with explicit original_server_id override
    logger.info("Test 5: Path components with original_server_id override")
    server_dir, path_id = get_path_components(
        "unknown-uuid", 
        "unknown.host.com",
        original_server_id="9999"
    )
    logger.info(f"Override Path: {server_dir}, Path ID: {path_id}")
    assert path_id == "9999", f"Expected path_id 9999, got {path_id}"
    assert server_dir == "unknown.host.com_9999", f"Expected unknown.host.com_9999, got {server_dir}"
    
    # Test 6: Test with invalid UUID (should extract numbers or use as-is)
    logger.info("Test 6: Invalid UUID handling")
    server_id, is_known = identify_server("invalid-uuid-123456")
    logger.info(f"Invalid UUID: invalid-uuid-123456 -> Server ID: {server_id}, Is Known: {is_known}")
    assert server_id == "123456", f"Expected 123456, got {server_id}"
    assert not is_known, "Expected is_known to be False"
    
    # Test 7: Test with guild isolation
    logger.info("Test 7: Guild isolation check")
    server_dir1, path_id1 = get_path_components(
        original_uuid, 
        test_servers[0]["hostname"],
        guild_id="1219706687980568769"
    )
    server_dir2, path_id2 = get_path_components(
        original_uuid, 
        test_servers[0]["hostname"],
        guild_id="different-guild-id"
    )
    logger.info(f"Guild 1: {server_dir1}, Path ID: {path_id1}")
    logger.info(f"Guild 2: {server_dir2}, Path ID: {path_id2}")
    
    # Clean up test data
    logger.info("Cleaning up test data")
    for server in test_servers:
        await db.game_servers.delete_one({"server_id": server["server_id"]})

async def main():
    """Main test function"""
    try:
        # Initialize database
        await initialize_db()
        
        # Set up test data
        test_servers = await setup_test_data()
        
        # Run tests
        await perform_tests(test_servers)
        
        logger.info("All tests completed successfully")
        return 0
    except Exception as e:
        logger.error(f"Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))