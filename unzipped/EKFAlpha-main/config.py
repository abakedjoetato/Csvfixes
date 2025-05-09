# Configuration for Tower of Temptation PvP Statistics Bot

# Premium tier configuration
PREMIUM_TIERS = {
    0: {
        "name": "Scavenger",
        "max_servers": 1,
        "features": ["killfeed"],
        "price": 0  # Free tier
    },
    1: {
        "name": "Survivor",
        "max_servers": 1,
        "features": ["killfeed", "basic_stats", "leaderboards"],
        "price": 5  # £5 per month
    },
    2: {
        "name": "Mercenary",
        "max_servers": 2,
        "features": ["killfeed", "basic_stats", "leaderboards", "rivalries", "bounties", "player_links", "economy", "advanced_analytics"],
        "price": 15  # £15 per month
    },
    3: {
        "name": "Warlord",
        "max_servers": 3,
        "features": ["killfeed", "basic_stats", "leaderboards", "rivalries", "bounties", "player_links", "factions", "economy", "advanced_analytics"],
        "price": 25  # £25 per month
    },
    4: {
        "name": "Overseer",
        "max_servers": 10,  # Higher limit
        "features": ["killfeed", "basic_stats", "leaderboards", "rivalries", "bounties", "player_links", "factions", "economy", "advanced_analytics"],
        "price": 50  # £50 per month
    }
}

# Default colors for Discord embeds
DEFAULT_COLOR_PRIMARY = "#7289DA"   # Discord blurple
DEFAULT_COLOR_SECONDARY = "#FFFFFF" # White
DEFAULT_COLOR_ACCENT = "#23272A"    # Discord dark

# Default PvP statistics settings
DEFAULT_STATS_CONFIG = {
    "kd_ratio_min_kills": 5,         # Minimum kills required to calculate K/D ratio
    "significant_rivalry_kills": 3,   # Minimum kills to consider a rivalry significant
    "top_players_limit": 10,          # Default number of players to show in leaderboards
    "auto_bounty_threshold": 5,       # Kills needed to trigger auto-bounty
    "default_bounty_reward": 100,     # Default reward for bounties 
    "default_expiration_hours": 24,   # Default bounty expiration time in hours
}

# Command prefix for traditional commands
COMMAND_PREFIX = "!"

# Default embed colors
EMBED_COLOR = "#7289DA"
EMBED_COLOR_ERROR = "#FF0000"
EMBED_COLOR_SUCCESS = "#00FF00"
EMBED_COLOR_WARNING = "#FFFF00"
EMBED_COLOR_INFO = "#0000FF"

# Default bot settings
BOT_VERSION = "1.0.0"
SUPPORT_SERVER_INVITE = "https://discord.gg/example"
SUPPORT_CONTACT = "support@example.com"

# CSV Parsing constants
CSV_FIELDS = {
    "timestamp": 0,
    "killer_name": 1,
    "killer_id": 2,
    "victim_name": 3,
    "victim_id": 4,
    "weapon": 5,
    "distance": 6,
    "killer_console": 7,
    "victim_console": 8
}

# CSV filename pattern for identifying CSV files
CSV_FILENAME_PATTERN = r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}\.csv$"

# Event patterns for log parsing
EVENT_PATTERNS = {
    "player_joined": r"Player (\w+) \(([0-9a-f]+)\) connected",
    "player_left": r"Player (\w+) \(([0-9a-f]+)\) disconnected",
    "server_restart": r"Server is restarting",
    "admin_command": r"Admin command: (.*) by (.*)",
    "game_event": r"Game event: (.*)"
}

# Embed themes
EMBED_THEMES = {
    "default": {
        "color": "#7289DA",
        "footer": "Tower of Temptation PvP Statistics",
        "icon": "https://i.imgur.com/example.png"
    },
    "error": {
        "color": "#FF0000",
        "footer": "Error | Tower of Temptation",
        "icon": "https://i.imgur.com/error.png"
    },
    "success": {
        "color": "#00FF00",
        "footer": "Success | Tower of Temptation",
        "icon": "https://i.imgur.com/success.png"
    }
}

# Embed footer and icon
EMBED_FOOTER = "Tower of Temptation PvP Statistics"
EMBED_ICON = "https://i.imgur.com/example.png"
