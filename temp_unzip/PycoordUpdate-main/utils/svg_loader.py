"""
Utility for providing emoji symbols for Deadside-themed gambling games
"""
import logging
from typing import Dict, Optional, List, Tuple

logger = logging.getLogger(__name__)

# Deadside-themed emoji mapping
DEADSIDE_EMOJIS = {
    "pistol": "🔫",         # Pistol (IZH-70, TTk, etc.)
    "rifle": "🪖",          # Assault rifle (AK-SU, AR4, etc.)
    "medkit": "🩹",         # Medical supplies
    "ammo": "🧰",           # Ammunition box
    "backpack": "🎒",       # Tactical backpack
    "food": "🥫",           # Canned survival food
    "helmet": "⛑️",         # Combat helmet
    "dogtag": "🏷️",         # Dog tags from fallen players
    "emerald": "💎"         # Emerald crystal (premium item)
}

# Function aliases to maintain compatibility with existing code
def load_svg_content(icon_name: str) -> Optional[str]:
    """Legacy function - now returns emoji char instead of SVG content"""
    return DEADSIDE_EMOJIS.get(icon_name, "❓")

def get_svg_as_data_url(icon_name: str) -> Optional[str]:
    """Legacy function - now simply returns the emoji as string"""
    return DEADSIDE_EMOJIS.get(icon_name, "❓")

def get_all_svg_icons() -> List[str]:
    """Get a list of all available icon names"""
    return list(DEADSIDE_EMOJIS.keys())

def get_svg_symbol_data() -> List[Tuple[str, str, str]]:
    """Get data for slot symbols with names and descriptions"""
    symbols = [
        # icon_name, display_name, description
        ("pistol", "Survival Pistol", "Standard sidearm for Deadside encounters"),
        ("rifle", "Tactical Rifle", "High-powered weapon for eliminating threats"),
        ("medkit", "Survival Medkit", "First aid supplies essential for survival"),
        ("ammo", "Ammunition Crate", "Extra rounds for your arsenal"),
        ("backpack", "Tactical Backpack", "Storage for your valuable Deadside loot"),
        ("food", "Emergency Rations", "Essential survival food in the wasteland"),
        ("helmet", "Combat Helmet", "Protection from headshots in combat"),
        ("dogtag", "Survivor's Tags", "ID tags from fallen Deadside warriors"),
        ("emerald", "Emerald Artifact", "Extremely rare gem with mysterious properties")
    ]
    
    return symbols