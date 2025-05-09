"""
Fix CSV Parser Delimiter Handling

This script fixes the issue with the CSV parser not accepting a
delimiter parameter, which appears to be causing parsing failures
during historical parsing.
"""
import asyncio
import logging
import os
import sys
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("fix_csv_parser_delimiter.log", mode="w"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

async def main():
    """Fix the CSV parser and processor delimiter handling mismatch"""
    try:
        # Import discord module for messaging
        import discord
        
        # Post initial status
        await post_discord_status("Fixing CSV parser delimiter handling...", discord.Color.blue())
        
        # Step 1: Check the CSVParser class in utils/csv_parser.py
        logger.info("Checking CSV parser implementation in utils/csv_parser.py")
        
        with open("utils/csv_parser.py", "r") as f:
            parser_content = f.read()
        
        # Look for parse_csv_file or _parse_csv_file method
        parse_method_found = False
        if "_parse_csv_file" in parser_content:
            logger.info("Found _parse_csv_file method in CSVParser")
            parse_method_found = True
        elif "parse_csv_file" in parser_content:
            logger.info("Found parse_csv_file method in CSVParser")
            parse_method_found = True
        
        if not parse_method_found:
            logger.error("Could not find parsing method in CSVParser")
            await post_discord_status(
                "❌ Could not find CSV parsing method in utils/csv_parser.py",
                discord.Color.red()
            )
            return
        
        # Step 2: Check if the method already handles delimiter parameter
        accepts_delimiter = "delimiter" in parser_content and "self._parse_csv_file" in parser_content
        
        # Step 3: Check the csv_processor.py file for delimiter usage
        logger.info("Checking CSV processor for delimiter usage in cogs/csv_processor.py")
        
        with open("cogs/csv_processor.py", "r") as f:
            processor_content = f.read()
        
        passes_delimiter = "delimiter" in processor_content and "parse_csv_data" in processor_content
        
        if passes_delimiter:
            logger.info("CSV processor is trying to pass delimiter parameter")
            
            # Check if the parser doesn't accept it (this is the mismatch)
            if not accepts_delimiter:
                logger.info("Found mismatch: Processor passes delimiter but Parser doesn't accept it")
                
                # Fix: Create backup of parser file
                parser_backup_path = "utils/csv_parser.py.bak"
                with open(parser_backup_path, "w") as f:
                    f.write(parser_content)
                logger.info(f"Created backup of CSV parser at {parser_backup_path}")
                
                # Fix: Add delimiter parameter to parse methods
                fixed_parser_content = fix_parser_delimiter(parser_content)
                
                # Write fixed parser content
                with open("utils/csv_parser.py", "w") as f:
                    f.write(fixed_parser_content)
                logger.info("Fixed CSV parser to accept delimiter parameter")
                
                # Test the fix
                success = await test_csv_parsing()
                
                if success:
                    await post_discord_status(
                        "✅ Successfully fixed CSV parser delimiter handling!\n" +
                        "The CSV files should now parse correctly during historical processing.",
                        discord.Color.green()
                    )
                    logger.info("Successfully fixed and verified CSV parser delimiter handling")
                else:
                    # Restore backup
                    with open("utils/csv_parser.py", "w") as f:
                        f.write(parser_content)
                    logger.error("Parser fix verification failed, restored backup")
                    
                    await post_discord_status(
                        "❌ CSV parser fix failed verification. Original file restored.",
                        discord.Color.red()
                    )
            else:
                logger.info("No mismatch found: Both processor and parser handle delimiter")
                await post_discord_status(
                    "ℹ️ No delimiter handling mismatch found in CSV parser and processor",
                    discord.Color.blue()
                )
        else:
            logger.info("CSV processor does not pass delimiter parameter")
            await post_discord_status(
                "ℹ️ CSV processor does not pass delimiter parameter",
                discord.Color.blue()
            )
        
    except Exception as e:
        logger.error(f"Error fixing CSV parser: {e}")
        import traceback
        traceback.print_exc()
        
        try:
            await post_discord_status(
                f"❌ Error fixing CSV parser: {str(e)}",
                discord.Color.red()
            )
        except:
            pass

def fix_parser_delimiter(content):
    """Fix the CSV parser to correctly handle the delimiter parameter"""
    # Check where we need to add the delimiter parameter
    if "_parse_csv_file" in content:
        # Find the method definition
        method_start = content.find("def _parse_csv_file")
        if method_start == -1:
            logger.error("Could not find _parse_csv_file method definition")
            return content
        
        # Find the parameter list end
        param_end = content.find(")", method_start)
        if param_end == -1:
            logger.error("Could not find parameter list end for _parse_csv_file")
            return content
        
        # Check if delimiter parameter already exists
        method_def = content[method_start:param_end+1]
        if "delimiter" in method_def:
            logger.info("_parse_csv_file already has delimiter parameter")
            return content
        
        # Add delimiter parameter
        if "self" in method_def:
            # Insert delimiter after the last parameter
            last_param_pos = method_def.rfind(",")
            if last_param_pos == -1:
                # Only self parameter, add after it
                new_method_def = method_def.replace("self)", "self, delimiter=None)")
            else:
                # Multiple parameters, add after the last one
                new_method_def = method_def[:last_param_pos+1] + " delimiter=None" + method_def[last_param_pos+1:]
            
            # Update the method definition
            new_content = content[:method_start] + new_method_def + content[param_end+1:]
            
            # Now modify the method body to use the delimiter
            method_body_start = new_content.find(":", method_start)
            if method_body_start == -1:
                logger.error("Could not find method body start for _parse_csv_file")
                return new_content
            
            # Find first line of method body
            first_line_start = new_content.find("\n", method_body_start) + 1
            if first_line_start == 0:
                logger.error("Could not find first line of method body for _parse_csv_file")
                return new_content
            
            # Add code to use the delimiter parameter if provided
            indentation = get_indentation(new_content, first_line_start)
            delimiter_code = f"\n{indentation}# Use provided delimiter if specified\n"
            delimiter_code += f"{indentation}if delimiter is not None:\n"
            delimiter_code += f"{indentation}    self.delimiter = delimiter\n"
            
            # Insert the code at the beginning of the method body
            new_content = new_content[:first_line_start] + delimiter_code + new_content[first_line_start:]
            
            logger.info("Added delimiter parameter to _parse_csv_file method")
            return new_content
    
    # Also modify parse_csv_data to pass through the delimiter
    if "def parse_csv_data" in content:
        method_start = content.find("def parse_csv_data")
        if method_start == -1:
            logger.error("Could not find parse_csv_data method definition")
            return content
        
        # Find the parameter list end
        param_end = content.find(")", method_start)
        if param_end == -1:
            logger.error("Could not find parameter list end for parse_csv_data")
            return content
        
        # Check if delimiter parameter already exists
        method_def = content[method_start:param_end+1]
        if "delimiter" in method_def:
            logger.info("parse_csv_data already has delimiter parameter")
            return content
        
        # Add delimiter parameter
        if "self" in method_def:
            # Insert delimiter after the last parameter
            last_param_pos = method_def.rfind(",")
            if last_param_pos == -1:
                # Only basic parameters, add after them
                if "=" in method_def:  # Has optional parameters
                    # Find position of first optional parameter
                    equal_pos = method_def.find("=")
                    prev_comma = method_def.rfind(",", 0, equal_pos)
                    if prev_comma == -1:
                        new_method_def = method_def.replace("self, ", "self, delimiter=None, ")
                    else:
                        new_method_def = method_def[:prev_comma+1] + " delimiter=None," + method_def[prev_comma+1:]
                else:
                    new_method_def = method_def.replace("data)", "data, delimiter=None)")
            else:
                # Multiple parameters, add after the last one
                new_method_def = method_def[:last_param_pos+1] + " delimiter=None" + method_def[last_param_pos+1:]
            
            # Update the method definition
            content = content[:method_start] + new_method_def + content[param_end+1:]
            
            # Now find where the method calls _parse_csv_file
            if "_parse_csv_file" in content:
                call_pos = content.find("self._parse_csv_file", method_start)
                if call_pos != -1:
                    # Find the end of the argument list
                    call_args_end = content.find(")", call_pos)
                    if call_args_end != -1:
                        # Check if delimiter is already passed
                        call_args = content[call_pos:call_args_end+1]
                        if "delimiter" not in call_args:
                            # Add delimiter to the argument list
                            if "," in call_args:
                                # Multiple arguments, add delimiter after the last one
                                last_comma = call_args.rfind(",")
                                new_call_args = call_args[:last_comma+1] + " delimiter=delimiter" + call_args[last_comma+1:]
                            else:
                                # Only one argument, add delimiter after it
                                new_call_args = call_args.replace(")", ", delimiter=delimiter)")
                            
                            # Update the method call
                            content = content[:call_pos] + new_call_args + content[call_args_end+1:]
                            logger.info("Updated _parse_csv_file call to pass delimiter parameter")
    
    return content

def get_indentation(content, position):
    """Get the indentation at the given position in the content"""
    line_start = content.rfind("\n", 0, position) + 1
    return content[line_start:position]

async def test_csv_parsing():
    """Test the CSV parsing with the delimiter parameter"""
    try:
        # Import the CSV parser
        sys.path.append('.')
        from utils.csv_parser import CSVParser
        
        # Create test CSV data with semicolon delimiter
        test_data = """2025.05.09-11.58.37;Player1;ID1;Player2;ID2;weapon;10;PS4;PS4;
2025.05.09-12.01.22;Player3;ID3;Player4;ID4;weapon2;15;PC;PC;"""
        
        # Create parser instance
        parser = CSVParser()
        
        # Test parsing with explicit delimiter
        try:
            events = parser.parse_csv_data(test_data, delimiter=';')
            
            if not events or len(events) != 2:
                logger.error(f"Parser returned unexpected number of events: {len(events) if events else 0}")
                return False
            
            # Verify the parsed events
            if not isinstance(events[0].get('timestamp'), datetime):
                logger.error(f"Timestamp not correctly parsed: {events[0].get('timestamp')}")
                return False
            
            if events[0].get('killer_name') != 'Player1' or events[0].get('victim_name') != 'Player2':
                logger.error(f"Player names not correctly parsed: {events[0].get('killer_name')}, {events[0].get('victim_name')}")
                return False
            
            logger.info("CSV parser successfully handled delimiter parameter")
            logger.info(f"Parsed {len(events)} events with correct timestamps and player data")
            return True
            
        except TypeError as e:
            if "unexpected keyword argument 'delimiter'" in str(e):
                logger.error("Parser still doesn't accept delimiter parameter")
                return False
            logger.error(f"Parsing error: {e}")
            return False
            
        except Exception as e:
            logger.error(f"Unexpected error during parsing test: {e}")
            return False
        
    except Exception as e:
        logger.error(f"Error testing CSV parsing: {e}")
        import traceback
        traceback.print_exc()
        return False

async def post_discord_status(message, color=None):
    """Post a status message to Discord"""
    try:
        import discord
        import os
        
        # Get Discord token
        token = os.environ.get('DISCORD_TOKEN')
        if not token:
            logger.error("No Discord token found in environment variables")
            return
        
        # Create Discord client
        intents = discord.Intents.default()
        client = discord.Client(intents=intents)
        
        @client.event
        async def on_ready():
            try:
                logger.info(f"Connected to Discord as {client.user}")
                
                # Get target channel
                channel_id = 1360632422957449237  # bot-2 channel
                channel = client.get_channel(channel_id)
                
                if not channel:
                    try:
                        channel = await client.fetch_channel(channel_id)
                    except Exception as e:
                        logger.error(f"Error fetching channel: {e}")
                        await client.close()
                        return
                
                if not channel:
                    logger.error(f"Could not find channel with ID: {channel_id}")
                    await client.close()
                    return
                
                # Create embed
                embed = discord.Embed(
                    title="CSV Parser Delimiter Fix",
                    description=message,
                    color=color or discord.Color.blue()
                )
                
                # Add timestamp
                embed.timestamp = datetime.now()
                embed.set_footer(text="Tower of Temptation PvP Statistics Bot")
                
                # Send message
                await channel.send(embed=embed)
                logger.info("Posted status message to Discord")
                
                # Close client
                await client.close()
                
            except Exception as e:
                logger.error(f"Error posting to Discord: {e}")
                import traceback
                traceback.print_exc()
                await client.close()
        
        # Start client
        await client.start(token)
        
    except Exception as e:
        logger.error(f"Error creating Discord client: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())