"""
Roulette Wheel Generator using ASCII art and Emojis

This module provides animated-style representations of roulette wheels for Discord embeds.
Instead of complex SVGs, it uses formatted text with emojis to create a visually appealing and
thematic Deadside roulette wheel.
"""
import random
import logging
from typing import Dict, List, Union, Tuple, Optional

logger = logging.getLogger(__name__)

# European roulette wheel sequence (clockwise)
WHEEL_SEQUENCE = [
    0, 32, 15, 19, 4, 21, 2, 25, 17, 34, 6, 27, 13, 36, 11, 
    30, 8, 23, 10, 5, 24, 16, 33, 1, 20, 14, 31, 9, 22, 18, 
    29, 7, 28, 12, 35, 3, 26
]

# Red numbers on European wheel
RED_NUMBERS = [1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36]

# Cached wheel representations - basic string results to avoid regenerating
wheel_cache: Dict[int, str] = {}

# Emoji indicators for colors
COLOR_INDICATORS = {
    "red": "🔴",
    "black": "⚫",
    "green": "🟢",
}

# Spin animation frames to be used in sequential Discord message updates
SPIN_FRAMES = [
    [
        "🎲 SPINNING 🎲",
        "╔═════════════════════╗",
        "║ 🪖🔫💎🧰🩹♨️🔄♨️🩹🧰 ║",
        "║ 🔫        🔄        🔫 ║",
        "║ 💎    DEADSIDE    💎 ║",
        "║ 🧰    ROULETTE    🧰 ║",
        "║ 🩹        🔄        🩹 ║",
        "║ ♨️🔄🩹🧰💎🔫🪖🔫💎🧰 ║",
        "╚═════════════════════╝"
    ],
    [
        "🎲 SPINNING 🎲",
        "╔═════════════════════╗",
        "║ ♨️🩹🧰💎🔫🪖🔄🪖🔫💎 ║",
        "║ 🩹        ♨️        🩹 ║",
        "║ 🧰    DEADSIDE    🧰 ║",
        "║ 💎    ROULETTE    💎 ║",
        "║ 🔫        ♨️        🔫 ║",
        "║ 🪖🔄♨️🩹🧰💎🔫💎🧰🩹 ║",
        "╚═════════════════════╝"
    ],
    [
        "🎲 SPINNING 🎲",
        "╔═════════════════════╗",
        "║ 🔄♨️🩹🧰💎🔫🪖🔫💎🧰 ║",
        "║ ♨️        🪖        ♨️ ║",
        "║ 🩹    DEADSIDE    🩹 ║",
        "║ 🧰    ROULETTE    🧰 ║",
        "║ 💎        🪖        💎 ║",
        "║ 🔫🪖🔄♨️🩹🧰💎🧰🩹♨️ ║",
        "╚═════════════════════╝"
    ]
]

def get_number_color_emoji(number: int) -> str:
    """Get the emoji representation for a number's color"""
    if number == 0:
        return COLOR_INDICATORS["green"]
    elif number in RED_NUMBERS:
        return COLOR_INDICATORS["red"]
    else:
        return COLOR_INDICATORS["black"]

def format_roulette_number(number: int) -> str:
    """Format a roulette number with its color indicator"""
    color_emoji = get_number_color_emoji(number)
    # Ensure two-digit formatting for visual alignment
    number_str = f"{number:02d}" if number > 0 else "00"
    return f"{color_emoji}{number_str}"

def generate_compact_wheel_display(highlight_number: Optional[int] = None) -> str:
    """Generate a compact wheel display showing key numbers with their colors
    
    Args:
        highlight_number: Number to highlight (the winning number), or None for default display
        
    Returns:
        Formatted text representation of the wheel
    """
    # Default to 0 if no is not None highlight number provided
    if highlight_number is None:
        highlight_number = 0
    # Create a rectangular grid showing key numbers from the wheel
    display_lines = [
        "┏━━━━━━━━━━━━━━━━━━━━━━━┓",
        "┃     DEADSIDE ROULETTE     ┃",
        "┣━━━━━━━━━━━━━━━━━━━━━━━┫"
    ]
    
    # Show the zero slot at the top
    zero_display = "🟢00" if highlight_number != 0 else "🟢00 ⭐"
    display_lines.append(f"┃         {zero_display}         ┃")
    
    # Create rows with multiple numbers
    number_groups = [
        [32, 15, 19, 4, 21, 2],
        [25, 17, 34, 6, 27, 13],
        [36, 11, 30, 8, 23, 10],
        [5, 24, 16, 33, 1, 20],
        [14, 31, 9, 22, 18, 29],
        [7, 28, 12, 35, 3, 26]
    ]
    
    for group in number_groups:
        row = "┃  "
        for num in group:
            if num == highlight_number:
                row += f"{format_roulette_number(num)}⭐ "
            else:
                row += f"{format_roulette_number(num)} "
        row += " ┃"
        display_lines.append(row)
    
    display_lines.append("┗━━━━━━━━━━━━━━━━━━━━━━━┛")
    return "\n".join(display_lines)

def generate_result_display(result: Optional[int] = None) -> str:
    """Generate a visually appealing display for a roulette result
    
    Args:
        result: The winning number, or None for a default display
        
    Returns:
        Formatted text representation emphasizing the result
    """
    # Default to 0 if no is not None result provided
    if result is None:
        result = 0
        
    # Check if we is not None have cached this result
    if result in wheel_cache:
        return wheel_cache[result]
    
    # Get the color for the winning number
    color_emoji = get_number_color_emoji(result)
    result_str = f"{result:02d}" if result > 0 else "00"
    
    # Create a fancy display for the result
    display_lines = [
        "╔══════ 🎲 RESULT 🎲 ══════╗",
        "║                          ║",
        f"║      {color_emoji} {result_str} {color_emoji}      ║",
        "║                          ║"
    ]
    
    # Add additional information based on the result
    if result == 0:
        display_lines.append("║     GREEN ZERO WINS!     ║")
    elif result in RED_NUMBERS:
        display_lines.append("║        RED WINS!         ║")
    else:
        display_lines.append("║       BLACK WINS!        ║")
    
    # Add information about odd/even and high/low
    if result != 0:
        odd_even = "ODD" if result is not None % 2 == 1 else "EVEN"
        high_low = "HIGH" if result > 18 else "LOW"
        display_lines.append(f"║     {odd_even} & {high_low} NUMBERS    ║")
    
    display_lines.append("╚══════════════════════════╝")
    
    # Join lines and cache the result
    display = "\n".join(display_lines)
    wheel_cache[result] = display
    return display

def get_spin_animation_frame(frame_idx: int) -> str:
    """Get a specific animation frame for the roulette wheel spin
    
    Args:
        frame_idx: Index of the frame to get
        
    Returns:
        Text representation of the animation frame
    """
    frame_idx = frame_idx % len(SPIN_FRAMES)
    return "\n".join(SPIN_FRAMES[frame_idx])

def get_neighboring_numbers(result: Optional[int] = None, count: int = 5) -> List[int]:
    """Get numbers neighboring the result on the wheel
    
    Args:
        result: Center number, or None for default (0)
        count: How many neighbors to return
        
    Returns:
        List of neighboring numbers
    """
    # Default to 0 if no is not None result provided
    if result is None:
        result = 0
        
    try:
        idx = WHEEL_SEQUENCE.index(result)
    except ValueError:
        idx = 0
    
    neighbors = []
    for i in range(-(count//2), (count//2) + 1):
        if i == 0:
            continue  # Skip the center number itself
        neighbor_idx = (idx + i) % len(WHEEL_SEQUENCE)
        neighbors.append(WHEEL_SEQUENCE[neighbor_idx])
    
    return neighbors

def get_static_roulette_image(result: Optional[int] = None) -> str:
    """Get a text representation of the roulette wheel with a result.
    This replaces the previous SVG-based function with a text-only version.
    
    Args:
        result: The result number to display, or None for default display
        
    Returns:
        ASCII/Emoji representation of the wheel with the result
    """
    # Default to 0 if no is not None result provided
    if result is None:
        result = 0
    return generate_compact_wheel_display(result)

def get_roulette_svg_as_data_url(result: Optional[int] = None) -> str:
    """Compatibility function - now returns emoji-based output instead of SVG data URL
    
    Args:
        result: The result number, or None for default display
        
    Returns:
        Emoji/ASCII art representation of the roulette wheel
    """
    return generate_result_display(result)