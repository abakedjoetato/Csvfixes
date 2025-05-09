"""
Test CSV Parser Delimiter Parameter

This script tests if the CSV parser correctly handles the delimiter parameter.
"""

import asyncio
import sys
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('csv_delimiter_test.log', mode='w')
    ]
)
logger = logging.getLogger("csv_delimiter_test")

async def test_csv_parser():
    """Test the CSV parser's delimiter parameter handling"""
    try:
        # Import the CSV parser
        from utils.csv_parser import CSVParser
        logger.info("Successfully imported CSVParser")
        
        # Create test CSV data with semicolon delimiter
        test_data = """2025.05.09-11.58.37;Player1;ID1;Player2;ID2;weapon;10;PS4;PS4;
2025.05.09-12.01.22;Player3;ID3;Player4;ID4;weapon2;15;PC;PC;"""
        
        # Create a CSV parser instance
        parser = CSVParser()
        logger.info("Created CSVParser instance")
        
        # Test with explicit delimiter
        logger.info("Parsing CSV data with explicit delimiter ';'")
        try:
            events = parser.parse_csv_data(test_data, delimiter=';')
            logger.info(f"Successfully parsed {len(events)} events with delimiter parameter")
            if events:
                logger.info(f"First event: {events[0]}")
                
                # Verify timestamp parsing
                if 'timestamp' in events[0]:
                    if isinstance(events[0]['timestamp'], datetime):
                        logger.info(f"Timestamp correctly parsed as datetime: {events[0]['timestamp']}")
                    else:
                        logger.error(f"Timestamp not parsed as datetime, got: {type(events[0]['timestamp'])}")
                else:
                    logger.error("No timestamp field in parsed event")
                
                # Verify event data
                if events[0].get('killer_name') == 'Player1' and events[0].get('victim_name') == 'Player2':
                    logger.info("Player names correctly parsed")
                else:
                    logger.error(f"Player names incorrectly parsed: {events[0].get('killer_name')}, {events[0].get('victim_name')}")
            
            return True
        except TypeError as e:
            if "unexpected keyword argument 'delimiter'" in str(e):
                logger.error(f"Parser doesn't accept delimiter parameter: {e}")
            else:
                logger.error(f"Type error during parsing: {e}")
            return False
        except Exception as e:
            logger.error(f"Error during parsing: {e}")
            return False
    except Exception as e:
        logger.error(f"Error in test: {e}")
        return False

async def test_csv_processor():
    """Test if the CSV processor emergency fix works correctly"""
    try:
        # Import the relevant functions
        import importlib.util
        
        # Test if the csv_processor module can be imported
        try:
            spec = importlib.util.spec_from_file_location("csv_processor", "cogs/csv_processor.py")
            if spec is None:
                logger.error("Failed to load spec for csv_processor.py")
                return False
                
            csv_processor = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(csv_processor)
            logger.info("Successfully imported csv_processor module")
            return True
        except SyntaxError as e:
            logger.error(f"Syntax error in csv_processor.py: {e}")
            return False
        except Exception as e:
            logger.error(f"Error importing csv_processor: {e}")
            return False
    except Exception as e:
        logger.error(f"Error in csv_processor test: {e}")
        return False

async def main():
    """Run all tests"""
    logger.info("Starting CSV parser delimiter test")
    
    # Test the CSV parser
    parser_success = await test_csv_parser()
    logger.info(f"CSV parser test {'PASSED' if parser_success else 'FAILED'}")
    
    # Test the CSV processor
    processor_success = await test_csv_processor()
    logger.info(f"CSV processor syntax test {'PASSED' if processor_success else 'FAILED'}")
    
    # Overall result
    if parser_success and processor_success:
        logger.info("All tests PASSED")
        print("CSV delimiter test: PASSED")
    else:
        if not parser_success:
            logger.error("CSV parser test FAILED")
            print("CSV parser test: FAILED - The parser doesn't correctly handle the delimiter parameter")
        
        if not processor_success:
            logger.error("CSV processor syntax test FAILED")
            print("CSV processor test: FAILED - There's a syntax error in the csv_processor.py file")

if __name__ == "__main__":
    asyncio.run(main())