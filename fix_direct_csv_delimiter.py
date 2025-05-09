"""
Fix for CSV Parser Delimiter Parameter

This script adds a delimiter parameter to the _parse_csv_file method in utils/csv_parser.py
to fix the issue with the emergency fix code in csv_processor.py.
"""
import os
from datetime import datetime

def main():
    """Main fix function"""
    print("Starting CSV parser delimiter fix")
    
    # Make a backup
    try:
        csv_parser_path = "utils/csv_parser.py"
        csv_parser_backup = f"utils/csv_parser.py.bak.{int(datetime.now().timestamp())}"
        
        with open(csv_parser_path, "r") as f:
            original_content = f.read()
            
        # Create backup
        with open(csv_parser_backup, "w") as f:
            f.write(original_content)
        print(f"Created backup at {csv_parser_backup}")
        
        # Check if the method exists
        method_signature_marker = "def _parse_csv_file(self, file: Union[TextIO, BinaryIO], file_path: str = None, only_new_lines: bool = False)"
        
        if method_signature_marker in original_content:
            print("Found _parse_csv_file method signature")
            
            # Replace method signature to include delimiter parameter
            new_signature = "def _parse_csv_file(self, file: Union[TextIO, BinaryIO], file_path: str = None, only_new_lines: bool = False, delimiter: str = None)"
            modified_content = original_content.replace(method_signature_marker, new_signature)
            
            # Add code to use the delimiter parameter
            # Find where we detect the delimiter
            delimiter_detection_marker = "# Detect CSV format (separator and columns)"
            
            if delimiter_detection_marker in modified_content:
                print("Found delimiter detection section")
                
                # Find the start of that section
                delimiter_section_start = modified_content.find(delimiter_detection_marker)
                
                # Find the method signature end and add the delimiter handling code
                method_body_start = modified_content.find(":", modified_content.find("_parse_csv_file"))
                
                # Get the indentation of the first line
                next_line_start = modified_content.find("\n", method_body_start) + 1
                indentation = ""
                for char in modified_content[next_line_start:]:
                    if char in [' ', '\t']:
                        indentation += char
                    else:
                        break
                
                # Add code to handle the delimiter parameter
                delimiter_handling_code = f"""
{indentation}# Use provided delimiter if specified
{indentation}if delimiter is not None:
{indentation}    self.delimiter = delimiter
{indentation}    logger.info(f"Using provided delimiter: '{delimiter}'")
"""
                
                # Insert the code right after the method opens
                insertion_point = modified_content.find("\n", method_body_start) + 1
                modified_content = modified_content[:insertion_point] + delimiter_handling_code + modified_content[insertion_point:]
                
                # Also modify parse_csv_data to pass the delimiter through
                parse_data_signature = "def parse_csv_data(self, data: Union[str, bytes])"
                if parse_data_signature in modified_content:
                    print("Found parse_csv_data method")
                    
                    # Update the signature
                    new_parse_data_signature = "def parse_csv_data(self, data: Union[str, bytes], delimiter: str = None)"
                    modified_content = modified_content.replace(parse_data_signature, new_parse_data_signature)
                    
                    # Find where it calls _parse_csv_file
                    parse_call_marker = "events = self._parse_csv_file(csv_file)"
                    if parse_call_marker in modified_content:
                        print("Found _parse_csv_file call in parse_csv_data")
                        new_parse_call = "events = self._parse_csv_file(csv_file, delimiter=delimiter)"
                        modified_content = modified_content.replace(parse_call_marker, new_parse_call)
                    
                # Write the modified content
                with open(csv_parser_path, "w") as f:
                    f.write(modified_content)
                
                print("Successfully updated CSV parser to accept delimiter parameter")
                return True
            else:
                print("Could not find delimiter detection section")
                return False
        else:
            print("Could not find _parse_csv_file method signature")
            return False
            
    except Exception as e:
        print(f"Error fixing CSV parser: {e}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)