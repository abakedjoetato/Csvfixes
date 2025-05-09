# Emeralds Killfeed PvP Statistics Bot

A robust Discord bot for tracking PvP gameplay data, offering multi-guild isolation, player statistics, and advanced rivalry tracking with comprehensive debugging tools. The bot is designed to be completely dynamic with no hardcoded server details.

## Features

- **PvP Kill Tracking**: Real-time monitoring and detailed statistics for player kills
- **Multi-Guild Support**: Complete isolation of data between Discord servers
- **Player Rivalries**: Track nemesis and prey relationships between players
- **Bounty System**: Place bounties on players and claim rewards for fulfilling them
- **Server Statistics**: Detailed statistics for each game server
- **Historical Parsing**: Process historical game logs to build a comprehensive statistics database

## Running the Bot on Replit

### Quick Start Guide

1. **Start the Bot**: Simply click the **Run** button at the top of the Replit interface.
2. **Monitor Status**: Watch the console for startup messages. You should see:
   - "Bot logged in as Emeralds Killfeed"
   - "Connected to X guilds"
   - "Application commands synced!"
3. **Accessing the Bot**: The bot will appear online in your Discord server
4. **Stop the Bot**: Click the **Stop** button to terminate the process.

### Using the Discord Bot Workflow

The Discord bot runs using Replit workflows:

1. Go to the "Tools" tab in Replit
2. Select "Workflows" 
3. Click the "Run" button next to "Discord Bot"

### Environment Setup

The bot requires the following environment secret:
- `DISCORD_TOKEN`: Your Discord bot token

### Troubleshooting

If the bot fails to start:
- Check that the DISCORD_TOKEN is set correctly in Replit Secrets
- Verify the bot has proper permissions in your Discord server
- Check the Workflow console for any error messages

## Bot Architecture

The Emeralds Killfeed PvP Statistics Bot is designed with a modular architecture:

### Discord Bot Components

The Discord bot is responsible for:
- Interacting with users via Discord commands
- Processing game statistics from CSV files
- Managing player leaderboards and rivalries
- Handling the bounty system
- Providing real-time statistics via Discord embeds

## Environment Variables

The bot requires the following environment variables:

- `DISCORD_TOKEN`: Your Discord bot token
- `BOT_APPLICATION_ID`: Your Discord application ID  
- `HOME_GUILD_ID`: Discord ID of your home/main guild
- `MONGODB_URI`: MongoDB connection string

## Multi-Guild Isolation

The Emeralds Killfeed PvP Statistics Bot is designed with robust multi-guild isolation:

- Each Discord server (guild) has its own isolated data
- Player statistics are tracked separately for each guild
- Server configurations are guild-specific
- Commands operate only on the guild where they're executed

This ensures that multiple Discord communities can use the bot without data leakage between them.

## Dynamic Server Identity System

The bot includes a sophisticated server identity management system:

- **UUID Resilience**: Maintains consistent server IDs even when UUIDs change due to server resets
- **Database-Driven Mappings**: All server identity mappings are loaded from the database, with no hardcoded values
- **Automatic Synchronization**: Server mappings are automatically updated during database synchronization
- **Path Consistency**: Ensures consistent directory paths for log and CSV processing regardless of UUID changes
- **Guild-Isolated Identities**: Two different guilds can use the same server ID without conflicts

## Database Architecture

The bot uses MongoDB for storage and retrieval of all data:

### MongoDB Collections
- **guilds**: Discord server configurations
- **game_servers**: Game server details including server IDs and SFTP information
- **servers**: Server identity mapping between UUIDs and original server IDs
- **players**: Player statistics and profiles
- **events**: Kill events and other game events
- **bounties**: Active and completed bounties
- **factions**: Team or faction information
- **rivalries**: Tracked player rivalries

## Bot Utilities

The Emeralds Killfeed PvP Statistics Bot includes several utilities for maintenance and management:

### Bot Management Scripts

```bash
# Start the Discord bot
./discord_bot.sh

# Apply all bot fixes
python comprehensive_bot_fixes.py
```

### Diagnostic Tools

```bash
# Verify bot startup and environment
python validate_bot_startup.py

# Test comprehensive functionality
python test_all_functionality.py

# Check multi-guild isolation
python test_multi_guild_isolation.py

# Diagnose server issues
python diagnose_server.py

# Test server identity system
python test_server_identity.py
```

### Database Tools

```bash
# Check database connectivity
python check_db.py

# Fix type inconsistencies in the database
python fix_server_validation.py
```

## Data Security

All server IDs, user IDs, and other sensitive identifiers are properly typed and validated throughout the codebase:

- Guild IDs are stored as integers in MongoDB
- Server IDs (for game servers) are stored as strings for compatibility
- All IDs are validated before use in database operations
- Input validation is performed on all user-submitted data

---

Emeralds Killfeed PvP Statistics Bot © 2025