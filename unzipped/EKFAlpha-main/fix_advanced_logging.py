with open('utils/advanced_logging.py', 'r') as file:
    content = file.read()

# Replace the strings
content = content.replace(
    "{'succeeded' if success is not None else 'failed'}", 
    "{'succeeded' if success else 'failed'}"
)

with open('utils/advanced_logging.py', 'w') as file:
    file.write(content)

print("Replacements completed")
