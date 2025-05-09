"""
Card SVG Generator for Blackjack game
"""
import os
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import logging
import base64

logger = logging.getLogger(__name__)

# Cache for card SVGs
card_svg_cache: Dict[str, str] = {}

def get_template_path() -> str:
    """Get the full path for the card template SVG"""
    current_dir = Path(__file__).parent
    root_dir = current_dir.parent
    template_path = root_dir / "static" / "icons" / "cards" / "card_template.svg"
    return str(template_path)

def get_card_back_path() -> str:
    """Get the full path for the card back SVG"""
    current_dir = Path(__file__).parent
    root_dir = current_dir.parent
    back_path = root_dir / "static" / "icons" / "cards" / "card_back.svg"
    return str(back_path)

def load_template() -> str:
    """Load the card template SVG"""
    template_path = get_template_path()
    try:
        with open(template_path, 'r') as file:
            return file.read()
    except Exception as e:
        logger.error(f"Error loading card template: {e}")
        return ""

def load_card_back() -> str:
    """Load the card back SVG"""
    back_path = get_card_back_path()
    try:
        with open(back_path, 'r') as file:
            return file.read()
    except Exception as e:
        logger.error(f"Error loading card back: {e}")
        return ""

def get_suit_symbol(suit: str) -> str:
    """Get the symbol for a card suit"""
    symbols = {
        "HEARTS": "♥",
        "DIAMONDS": "♦",
        "CLUBS": "♣",
        "SPADES": "♠"
    }
    return symbols.get(suit, "?")

def get_suit_class(suit: str) -> str:
    """Get the CSS class for a card suit"""
    if suit in ["HEARTS", "DIAMONDS"]:
        return "hearts diamonds"
    return "clubs spades"

def generate_center_design(value: str, suit: str) -> str:
    """Generate the center design for a card based on its value and suit"""
    suit_symbol = get_suit_symbol(suit)
    
    # For face cards (J, Q, K) and Ace
    if value in ["A", "J", "Q", "K"]:
        # Special designs for face cards
        if value == "A":
            return f'<text x="50" y="75" font-size="50" text-anchor="middle">{suit_symbol}</text>'
        elif value == "K":
            return f"""
            <text x="50" y="75" font-size="40" text-anchor="middle">{suit_symbol}</text>
            <text x="50" y="95" font-size="16" text-anchor="middle">KING</text>
            """
        elif value == "Q":
            return f"""
            <text x="50" y="75" font-size="40" text-anchor="middle">{suit_symbol}</text>
            <text x="50" y="95" font-size="16" text-anchor="middle">QUEEN</text>
            """
        elif value == "J":
            return f"""
            <text x="50" y="75" font-size="40" text-anchor="middle">{suit_symbol}</text>
            <text x="50" y="95" font-size="16" text-anchor="middle">JACK</text>
            """
    
    # For number cards (2-10)
    try:
        num_value = int(value)
        if 2 <= num_value <= 10:
            design = f'<text x="50" y="75" font-size="40" text-anchor="middle">{suit_symbol}</text>'
            if num_value > 4:
                # Add smaller symbols for higher number cards
                design += f'<text x="35" y="55" font-size="15" text-anchor="middle">{suit_symbol}</text>'
                design += f'<text x="65" y="55" font-size="15" text-anchor="middle">{suit_symbol}</text>'
            if num_value > 6:
                design += f'<text x="35" y="95" font-size="15" text-anchor="middle">{suit_symbol}</text>'
                design += f'<text x="65" y="95" font-size="15" text-anchor="middle">{suit_symbol}</text>'
            if num_value > 8:
                design += f'<text x="50" y="35" font-size="15" text-anchor="middle">{suit_symbol}</text>'
                design += f'<text x="50" y="115" font-size="15" text-anchor="middle">{suit_symbol}</text>'
            return design
    except ValueError:
        pass
    
    # Default design
    return f'<text x="50" y="75" font-size="30" text-anchor="middle">{suit_symbol}</text>'

def generate_card_svg(value: str, suit: str) -> str:
    """Generate an SVG for a card with the given value and suit"""
    cache_key = f"{value}_{suit}"
    if cache_key in card_svg_cache:
        return card_svg_cache[cache_key]
    
    template = load_template()
    if template is None:
        logger.error("Failed to load card template")
        return ""
    
    # Replace placeholders
    suit_symbol = get_suit_symbol(suit)
    suit_class = get_suit_class(suit)
    center_design = generate_center_design(value, suit)
    
    svg = template.replace("{{VALUE}}", value)
    svg = svg.replace("{{SUIT_SYMBOL}}", suit_symbol)
    svg = svg.replace("{{SUIT_CLASS}}", suit_class)
    svg = svg.replace("{{CENTER_DESIGN}}", center_design)
    # Set logo opacity - show faded logo behind card design
    svg = svg.replace("{{LOGO_OPACITY}}", "0.15")
    
    # Cache the result
    card_svg_cache[cache_key] = svg
    return svg

def get_card_back_svg() -> str:
    """Get the SVG for a card back"""
    # Check cache first
    cache_key = "CARD_BACK"
    if cache_key in card_svg_cache:
        return card_svg_cache[cache_key]
    
    # Load from file
    svg = load_card_back()
    if svg is not None:
        card_svg_cache[cache_key] = svg
    
    return svg

def get_card_svg_as_data_url(value: str, suit: str) -> str:
    """Convert card SVG to a data URL for use in embeds"""
    svg = generate_card_svg(value, suit)
    if svg is None:
        return ""
    
    # Encode the SVG content to base64
    svg_base64 = base64.b64encode(svg.encode('utf-8')).decode('utf-8')
    
    # Create a data URL
    data_url = f"data:image/svg+xml;base64,{svg_base64}"
    
    return data_url

def get_card_back_as_data_url() -> str:
    """Convert card back SVG to a data URL for use in embeds"""
    svg = get_card_back_svg()
    if svg is None:
        return ""
    
    # Encode the SVG content to base64
    svg_base64 = base64.b64encode(svg.encode('utf-8')).decode('utf-8')
    
    # Create a data URL
    data_url = f"data:image/svg+xml;base64,{svg_base64}"
    
    return data_url