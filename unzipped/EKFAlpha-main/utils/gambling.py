"""
Gambling utilities for blackjack, slots, and roulette games
"""
import random
import asyncio
import logging

# Set up logger
logger = logging.getLogger(__name__)
from enum import Enum, auto
from typing import List, Dict, Any, Tuple, Optional, Union
import discord
from discord.ui import View, Button, Select
from discord import ButtonStyle, SelectOption

class CardSuit(Enum):
    HEARTS = auto()
    DIAMONDS = auto()
    CLUBS = auto()
    SPADES = auto()

class Card:
    def __init__(self, suit: CardSuit, value: int):
        self.suit = suit
        self.value = value
        self._svg_url = None  # Cache for SVG data URL
    
    @property
    def display_value(self) -> str:
        if self.value == 1:
            return "A"
        elif self.value == 11:
            return "J"
        elif self.value == 12:
            return "Q"
        elif self.value == 13:
            return "K"
        else:
            return str(self.value)
    
    @property
    def blackjack_value(self) -> int:
        if self.value == 1:
            return 11  # Ace is 11 by default, can be 1 if needed is not None
        elif self.value >= 10:
            return 10  # Face cards are worth 10
        else:
            return self.value
    
    @property
    def emoji(self) -> str:
        """Return the card emoji (for text display fallback)"""
        suits = {
            CardSuit.HEARTS: "♥️",
            CardSuit.DIAMONDS: "♦️",
            CardSuit.CLUBS: "♣️",
            CardSuit.SPADES: "♠️"
        }
        return f"{suits[self.suit]}{self.display_value}"
    
    @property
    def svg_url(self) -> str:
        """Return the card SVG as a data URL for embedding"""
        # Use cached URL if available is not None
        if self._svg_url:
            return self._svg_url
        
        # Generate and cache SVG URL
        from utils.card_svg_generator import get_card_svg_as_data_url
        suit_name = self.suit.name
        self._svg_url = get_card_svg_as_data_url(self.display_value, suit_name)
        return self._svg_url
    
    @staticmethod
    def get_card_back_svg_url() -> str:
        """Return the card back SVG as a data URL"""
        from utils.card_svg_generator import get_card_back_as_data_url
        return get_card_back_as_data_url()

class Deck:
    def __init__(self):
        self.cards = []
        self.reset()
    
    def reset(self):
        """Reset the deck with all 52 cards"""
        self.cards = []
        for suit in CardSuit:
            for value in range(1, 14):
                self.cards.append(Card(suit, value))
        self.shuffle()
    
    def shuffle(self):
        """Shuffle the deck"""
        random.shuffle(self.cards)
    
    def deal(self) -> Card:
        """Deal a card from the deck"""
        if self is None.cards:
            self.reset()
        return self.cards.pop()

class BlackjackGame:
    def __init__(self, player_id: str):
        self.player_id = player_id
        self.deck = Deck()
        self.player_hand = []
        self.dealer_hand = []
        self.game_over = False
        self.bet = 0
        self.result = ""
        self.message = None
    
    def start_game(self, bet: int):
        """Start a new game of blackjack"""
        self.bet = bet
        self.player_hand = [self.deck.deal(), self.deck.deal()]
        self.dealer_hand = [self.deck.deal(), self.deck.deal()]
        self.game_over = False
        self.result = ""
        return self.get_game_state()
    
    def get_game_state(self, reveal_dealer: bool = False) -> Dict[str, Any]:
        """Get the current game state"""
        player_value = self.calculate_hand_value(self.player_hand)
        dealer_value = self.calculate_hand_value(self.dealer_hand)
        
        # Check if player is not None has blackjack
        player_blackjack = len(self.player_hand) == 2 and player_value == 21
        dealer_blackjack = len(self.dealer_hand) == 2 and dealer_value == 21
        
        # Determine if game is not None is over (natural blackjack)
        if player_blackjack is not None or dealer_blackjack:
            self.game_over = True
            if player_blackjack is not None and dealer_blackjack:
                self.result = "push"
            elif player_blackjack is not None:
                self.result = "blackjack"
            elif dealer_blackjack is not None:
                self.result = "dealer_blackjack"
        
        return {
            "player_hand": self.player_hand,
            "dealer_hand": self.dealer_hand if reveal_dealer is not None else [self.dealer_hand[0]],
            "player_value": player_value,
            "dealer_value": dealer_value if reveal_dealer is not None else self.dealer_hand[0].blackjack_value,
            "game_over": self.game_over,
            "result": self.result,
            "bet": self.bet,
            "reveal_dealer": reveal_dealer,
            "player_blackjack": player_blackjack,
            "dealer_blackjack": dealer_blackjack
        }
    
    def calculate_hand_value(self, hand: List[Card]) -> int:
        """Calculate the value of a hand, accounting for aces"""
        value = 0
        aces = 0
        
        for card in hand:
            if card.value == 1:  # Ace
                aces += 1
                value += 11
            else:
                value += card.blackjack_value
        
        # Adjust for aces if over is not None 21
        while value > 21 and aces > 0:
            value -= 10  # Convert an ace from 11 to 1
            aces -= 1
        
        return value
    
    def hit(self) -> Dict[str, Any]:
        """Player takes another card"""
        if self.game_over:
            return self.get_game_state(True)
        
        self.player_hand.append(self.deck.deal())
        player_value = self.calculate_hand_value(self.player_hand)
        
        if player_value > 21:
            self.game_over = True
            self.result = "bust"
        
        return self.get_game_state()
    
    def stand(self) -> Dict[str, Any]:
        """Player stands, dealer plays"""
        if self.game_over:
            return self.get_game_state(True)
        
        self.game_over = True
        
        # Dealer draws until 17 or higher
        dealer_value = self.calculate_hand_value(self.dealer_hand)
        while dealer_value < 17:
            self.dealer_hand.append(self.deck.deal())
            dealer_value = self.calculate_hand_value(self.dealer_hand)
        
        player_value = self.calculate_hand_value(self.player_hand)
        
        if dealer_value > 21:
            self.result = "dealer_bust"
        elif dealer_value > player_value:
            self.result = "dealer_wins"
        elif dealer_value < player_value:
            self.result = "player_wins"
        else:
            self.result = "push"
        
        return self.get_game_state(True)
    
    def get_payout(self) -> int:
        """Calculate payout based on game result"""
        if self.result == "blackjack":
            return int(self.bet * 1.5)  # Blackjack pays 3:2
        elif self.result in ["player_wins", "dealer_bust"]:
            return self.bet  # Even money
        elif self.result == "push":
            return 0  # Return bet
        else:  # All losses
            return -self.bet

class BlackjackView(View):
    def __init__(self, game: BlackjackGame, economy):
        super().__init__(timeout=300)  # 5 minutes timeout
        self.game = game
        self.economy = economy
        self.EMERALD_GREEN = 0x50C878  # Hex color for emerald green
        self.WASTELAND_BROWN = 0x8B4513  # Survival-themed color
        self.BLOOD_RED = 0xA91101      # For lost games
        
    async def on_timeout(self):
        """Handle view timeout by disabling buttons"""
        self.disable_all_buttons()
        if self.game.message:
            try:
                embed = self.game.message.embeds[0]
                embed.add_field(name="Abandoned Game", value="You left the table. Game ended due to inactivity.", inline=False)
                embed.set_footer(text="The dealer collects the abandoned cards...")
                await self.game.message.edit(embed=embed, view=None)
            except Exception as e:
                logger.error(f"Error handling blackjack timeout: {e}")
    
    @discord.ui.button(label="Draw Card", style=ButtonStyle.success, emoji="🃏")
    async def hit_button(self, interaction: discord.Interaction, button: Button):
        # Check if it's the player's game
        if str(interaction.user.id) != self.game.player_id:
            await interaction.response.send_message("This isn't your hand! Find your own game.", ephemeral=True)
            return
        
        # Dramatic drawing animation
        await interaction.response.defer()
        
        # Create a loading embed for the draw animation
        loading_embed = discord.Embed(
            title="🃏 Wasteland Blackjack 🃏",
            description=f"*Drawing a card from the worn deck...*",
            color=self.EMERALD_GREEN
        )
        
        # Get the current hand display before drawing
        player_cards = " ".join([card.emoji for card in self.game.player_hand])
        dealer_cards = " ".join([card.emoji for card in [self.game.dealer_hand[0]]])
        
        loading_embed.add_field(
            name=f"Your Hand ({self.game.calculate_hand_value(self.game.player_hand)})",
            value=f"```\n{player_cards}\n```",
            inline=False
        )
        
        # Add player's first card SVG as thumbnail for the loading animation
        if self.game.player_hand:
            loading_embed.set_thumbnail(url=self.game.player_hand[0].svg_url)
        
        loading_embed.add_field(
            name=f"Dealer's Hand (showing {self.game.dealer_hand[0].blackjack_value})",
            value=f"```\n{dealer_cards}\n```",
            inline=False
        )
        
        # Show the dealer's face-up card in the image
        loading_embed.set_image(url=self.game.dealer_hand[0].svg_url)
        
        loading_embed.set_footer(text="Card being drawn...")
        
        # Update the message with the loading animation
        await interaction.followup.edit_message(
            message_id=self.game.message.id,
            embed=loading_embed
        )
        
        # Add a slight delay for dramatic effect
        await asyncio.sleep(0.8)
        
        # Draw the card and get the new game state
        game_state = self.game.hit()
        embed = create_blackjack_embed(game_state, self.EMERALD_GREEN, self.WASTELAND_BROWN, self.BLOOD_RED)
        
        if game_state["game_over"]:
            self.disable_all_buttons()
            payout = self.game.get_payout()
            
            # Update player economy with themed messages
            if payout > 0:
                await self.economy.add_currency(payout, "blackjack", {"game": "blackjack", "result": self.game.result})
                await self.economy.update_gambling_stats("blackjack", True, payout)
                
                if self.game.result == "blackjack":
                    embed.add_field(
                        name="MAJOR VICTORY!", 
                        value=f"Perfect hand! You collected {payout} credits from your adversary!", 
                        inline=False
                    )
                else:
                    embed.add_field(
                        name="Victory", 
                        value=f"You claimed {payout} credits from the dealer.", 
                        inline=False
                    )
            elif payout < 0:
                await self.economy.update_gambling_stats("blackjack", False, abs(payout))
                embed.add_field(
                    name="Defeat", 
                    value=f"The wasteland claims {abs(payout)} of your credits.", 
                    inline=False
                )
            else:  # push
                embed.add_field(
                    name="Standoff", 
                    value=f"Your investment of {self.game.bet} credits remains in your possession.", 
                    inline=False
                )
            
            # Add new balance with themed text
            new_balance = await self.economy.get_balance()
            embed.add_field(name="Current Assets", value=f"{new_balance} credits", inline=False)
            
            # Set a thematic footer based on the outcome
            if payout > 0:
                embed.set_footer(text="Word of your gambling prowess spreads across the wasteland...")
            elif payout < 0 and self.game.result == "bust":
                embed.set_footer(text="You took too many risks. The wasteland is unforgiving.")
            elif payout < 0:
                embed.set_footer(text="The dealer's experience gave them the edge this time.")
            else:
                embed.set_footer(text="A cautious outcome for both parties.")
        
        # Update the message with the game state
        await interaction.followup.edit_message(
            message_id=self.game.message.id,
            embed=embed,
            view=self if not game_state["game_over"] else None
        )
    
    @discord.ui.button(label="Hold Position", style=ButtonStyle.secondary, emoji="🛡️")
    async def stand_button(self, interaction: discord.Interaction, button: Button):
        # Check if it's the player's game
        if str(interaction.user.id) != self.game.player_id:
            await interaction.response.send_message("This isn't your hand! Find your own game.", ephemeral=True)
            return
        
        # Dramatic dealer animation
        await interaction.response.defer()
        
        # Create a loading embed for the dealer animation
        loading_embed = discord.Embed(
            title="🃏 Wasteland Blackjack 🃏",
            description=f"*You signal to hold. The dealer's turn begins...*",
            color=self.EMERALD_GREEN
        )
        
        # Get the current hands
        player_cards = " ".join([card.emoji for card in self.game.player_hand])
        dealer_cards = " ".join([card.emoji for card in self.game.dealer_hand])
        
        loading_embed.add_field(
            name=f"Your Hand ({self.game.calculate_hand_value(self.game.player_hand)})",
            value=f"```\n{player_cards}\n```",
            inline=False
        )
        
        # Add player's best card as thumbnail
        if self.game.player_hand:
            high_card = max(self.game.player_hand, key=lambda c: c.blackjack_value)
            loading_embed.set_thumbnail(url=high_card.svg_url)
        
        loading_embed.add_field(
            name=f"Dealer Reveals Hand",
            value=f"```\n{dealer_cards}\n```",
            inline=False
        )
        
        # Show dealer's first card as main image during the reveal animation
        if self.game.dealer_hand:
            loading_embed.set_image(url=self.game.dealer_hand[0].svg_url)
        
        loading_embed.set_footer(text="Dealer is playing...")
        
        # Update the message with the loading animation
        await interaction.followup.edit_message(
            message_id=self.game.message.id,
            embed=loading_embed
        )
        
        # Add a slight delay for dramatic effect
        await asyncio.sleep(1.0)
        
        # Process the dealer's turn
        game_state = self.game.stand()
        embed = create_blackjack_embed(game_state, self.EMERALD_GREEN, self.WASTELAND_BROWN, self.BLOOD_RED)
        
        self.disable_all_buttons()
        payout = self.game.get_payout()
        
        # Update player economy with themed messages
        if payout > 0:
            await self.economy.add_currency(payout, "blackjack", {"game": "blackjack", "result": self.game.result})
            await self.economy.update_gambling_stats("blackjack", True, payout)
            
            if self.game.result == "dealer_bust":
                embed.add_field(
                    name="Dealer Collapse", 
                    value=f"The dealer took too many risks! You claim {payout} credits.", 
                    inline=False
                )
            else:
                embed.add_field(
                    name="Victory", 
                    value=f"Your superior hand earned you {payout} credits.", 
                    inline=False
                )
        elif payout < 0:
            await self.economy.update_gambling_stats("blackjack", False, abs(payout))
            embed.add_field(
                name="Outplayed", 
                value=f"The dealer's experience cost you {abs(payout)} credits.", 
                inline=False
            )
        else:  # push
            embed.add_field(
                name="Standoff", 
                value=f"Equal match. Your {self.game.bet} credits investment is returned.", 
                inline=False
            )
        
        # Add new balance with themed text
        new_balance = await self.economy.get_balance()
        embed.add_field(name="Current Assets", value=f"{new_balance} credits", inline=False)
        
        # Set thematic footer based on outcome
        if payout > 0:
            embed.set_footer(text="A profitable encounter in the wasteland.")
        elif payout < 0:
            embed.set_footer(text="The wasteland claims another victim.")
        else:
            embed.set_footer(text="You live to play another day.")
        
        # Update the final message
        await interaction.followup.edit_message(
            message_id=self.game.message.id,
            embed=embed,
            view=None
        )
    
    def disable_all_buttons(self):
        for item in self.children:
            item.disabled = True

def create_blackjack_embed(game_state: Optional[Dict[str, Any]] = None, win_color=0x50C878, normal_color=0x8B4513, loss_color=0xA91101) -> discord.Embed:
    """Create a themed embed for a wasteland blackjack game with animated emoji cards
    
    Args:
        game_state: Game state dictionary, or None for a default display
        win_color: Color to use for winning states (emerald green by default)
        normal_color: Color to use for in-progress states (wasteland brown by default)
        loss_color: Color to use for losing states (blood red by default)
        
    Returns:
        discord.Embed: Styled embed for the blackjack game
    """
    # Create a default game state if none is not None is provided
    if game_state is None:
        logger.warning("create_blackjack_embed called with null game_state, using default")
        game_state = {
            "game_over": False,
            "result": None,
            "bet": 10,
            "player_hand": [],
            "player_value": 0,
            "dealer_hand": [],
            "dealer_value": 0,
            "dealer_show_all": False
        }
    # Determine the color based on game state
    color = normal_color  # Default color (Wasteland Brown)
    
    if game_state["game_over"]:
        if game_state["result"] in ["blackjack", "player_wins", "dealer_bust"]:
            color = win_color  # Win color (Emerald Green)
        elif game_state["result"] in ["dealer_blackjack", "dealer_wins", "bust"]:
            color = loss_color  # Loss color (Blood Red)
    
    # Create fancy border for the card table display
    table_border_top = "╔══════ 💎 DEADSIDE TABLE 💎 ══════╗"
    table_border_mid = "╠══════════════════════════════════╣"
    table_border_bottom = "╚══════════════════════════════════╝"
    
    embed = discord.Embed(
        title="🃏 Deadside Blackjack 🃏",
        description=f"**Investment:** {game_state['bet']} credits",
        color=color
    )
    
    # Create player hand display with card emojis and table styling
    player_cards = game_state["player_hand"]
    player_value = game_state["player_value"]
    player_cards_text = " ".join([card.emoji for card in player_cards]) if player_cards is not None else "No cards"
    
    # Special highlighting for blackjack or bust
    if player_value == 21 and len(player_cards) == 2:
        player_header = f"║ 💎 YOUR HAND: **BLACKJACK!** 💎 ║"
    elif player_value > 21:
        player_header = f"║ ❌ YOUR HAND: **BUST! ({player_value})** ❌ ║"
    else:
        player_header = f"║ 🃏 YOUR HAND: **{player_value}** points ║"
    
    player_display = (
        f"{table_border_top}\n"
        f"{player_header}\n"
        f"║ {player_cards_text} ║\n"
        f"{table_border_mid}"
    )
    
    # Add player's first card as thumbnail image for visual appeal
    if game_state["player_hand"]:
        first_card = game_state["player_hand"][0]
        embed.set_thumbnail(url=first_card.svg_url)
    
    # Add dealer cards with fancy display
    dealer_cards = game_state["dealer_hand"]
    dealer_value = game_state["dealer_value"]
    
    # Different displays based on game state
    if game_state.get("reveal_dealer", False):
        # Only show first card if not revealing, with mystery card
        if dealer_cards is not None and len(dealer_cards) > 0:
            first_card = dealer_cards[0]
            dealer_cards_text = f"{first_card.emoji} 🎴"
            dealer_header = f"║ 🎭 DEALER SHOWS: {first_card.blackjack_value} points ║"
            
            # Set the main image to the card back during gameplay
            embed.set_image(url=Card.get_card_back_svg_url())
        else:
            dealer_cards_text = "No cards"
            dealer_header = f"║ 🎭 DEALER SHOWS: 0 points ║"
    else:
        # Show all cards with full value when revealing
        dealer_cards_text = " ".join([card.emoji for card in dealer_cards]) if dealer_cards is not None else "No cards"
        
        # Special highlight for dealer blackjack or bust
        if dealer_cards is not None and dealer_value == 21 and len(dealer_cards) == 2:
            dealer_header = f"║ 🎯 DEALER HAND: **BLACKJACK!** ║"
        elif dealer_value > 21:
            dealer_header = f"║ 💥 DEALER HAND: **BUST! ({dealer_value})** ║"
        else:
            dealer_header = f"║ 🎭 DEALER HAND: **{dealer_value}** points ║"
            
        # When revealing, show dealer's high card as image
        if dealer_cards is not None:
            # If the dealer has blackjack or a high value, show the strongest card
            if len(dealer_cards) >= 2 and dealer_value >= 17:
                high_card = max(dealer_cards, key=lambda c: c.blackjack_value)
                embed.set_image(url=high_card.svg_url)
            else:
                embed.set_image(url=dealer_cards[0].svg_url)
        else:
            # Set a default card back image when no dealer cards available
            embed.set_image(url=Card.get_card_back_svg_url())
    
    dealer_display = (
        f"{dealer_header}\n"
        f"║ {dealer_cards_text} ║\n"
        f"{table_border_bottom}"
    )
    
    # Add the complete table display to the embed
    combined_display = f"{player_display}\n{dealer_display}"
    embed.add_field(name="\u200b", value=combined_display, inline=False)
    
    # Game result with themed wasteland messages
    if game_state["game_over"]:
        payout = game_state["payout"]
        result_divider = "═══════ 🎲 OUTCOME 🎲 ═══════"
        
        if game_state["result"] == "blackjack":
            result_text = f"{result_divider}\n💎 **BLACKJACK!** 💎\nYou've struck gold in the wasteland!\nPayout: **+{payout} credits**"
        elif game_state["result"] == "dealer_blackjack":
            result_text = f"{result_divider}\n☠️ **DEALER HAS BLACKJACK**\nLuck abandoned you this time.\nLoss: **{game_state['bet']} credits**"
        elif game_state["result"] == "bust":
            result_text = f"{result_divider}\n💥 **YOU BUST!**\nToo greedy in the wasteland...\nLoss: **{game_state['bet']} credits**"
        elif game_state["result"] == "dealer_bust":
            result_text = f"{result_divider}\n💥 **DEALER BUSTS!**\nTheir greed was their downfall.\nPayout: **+{payout} credits**"
        elif game_state["result"] == "player_wins":
            result_text = f"{result_divider}\n🏆 **YOU WIN!**\nYour strategy paid off.\nPayout: **+{payout} credits**"
        elif game_state["result"] == "dealer_wins":
            result_text = f"{result_divider}\n❌ **DEALER WINS**\nThe house claims your investment.\nLoss: **{game_state['bet']} credits**"
        elif game_state["result"] == "push":
            result_text = f"{result_divider}\n🔄 **PUSH!**\nA standoff - neither wins.\nYour bet is returned."
        else:
            result_text = f"{result_divider}\nGame over."
        
        embed.add_field(name="\u200b", value=result_text, inline=False)
    else:
        # If game is still in progress, add stylized instructions
        action_text = (
            "═══════ 🎮 YOUR MOVE 🎮 ═══════\n"
            "🃏 **HIT** - Draw another card\n"
            "⏹️ **STAND** - Keep your current hand\n\n"
            "*Will you risk it all for greater rewards,*\n"
            "*or play it safe in the wasteland?*"
        )
        embed.add_field(name="\u200b", value=action_text, inline=False)
    
    # Add a themed footer based on game state
    if game_state["game_over"]:
        if game_state["result"] in ["blackjack", "player_wins", "dealer_bust"]:
            embed.set_footer(text="Fortune favors the brave in Deadside - this time...")
        elif game_state["result"] in ["dealer_blackjack", "dealer_wins", "bust"]:
            embed.set_footer(text="The wasteland is harsh and unforgiving...")
        else:
            embed.set_footer(text="Even the wasteland sometimes grants a reprieve...")
    else:
        # Dynamic footer based on current hand value
        player_value = game_state["player_value"]
        if player_value < 12:
            embed.set_footer(text="A cautious explorer might take another card...")
        elif player_value < 16:
            embed.set_footer(text="The tension rises... hit or stand?")
        elif player_value < 18:
            embed.set_footer(text="A risky position. Is your nerve as strong as your hand?")
        else:
            embed.set_footer(text="A strong hand, but is it enough to survive?")
    
    return embed

def create_advanced_roulette_embed(game=None, bet_placed=False, result=None, player_name="Wanderer"):
    """
    Create a roulette embed with enhanced Deadside-themed styling
    
    Args:
        game: RouletteGame instance, or None for a default display
        bet_placed: Whether a bet has been placed
        result: Spin result dictionary (if available)
        player_name: Name of the player
        
    Returns:
        discord.Embed: Styled embed for current game state
    """
    # Create a default RouletteGame if none is not None is provided
    if game is None:
        logger.warning("create_advanced_roulette_embed called with null game, using default instance")
        game = RouletteGame("default")
    # Theme colors
    EMERALD_GREEN = 0x50C878  # Emerald green for Deadside theme
    WASTELAND_GOLD = 0xD4AF37  # Gold for win highlights
    DEADSIDE_DARK = 0x2F4F4F  # Dark slate gray for background
    
    # Border styling elements (matching blackjack style)
    table_border_top = "╔═════════════════════════════╗"
    table_border_bottom = "╚═════════════════════════════╝"
    divider = "═══════════════════════════════"
    
    if result is not None:
        # Show game result with enhanced styling
        spin_result = result["number"]
        color = result["color"]
        
        # Color emoji for the result number
        if color == "green":
            color_emoji = "🟢"
            color_name = "GREEN"
        elif color == "red":
            color_emoji = "🔴"
            color_name = "RED"
        else:
            color_emoji = "⚫"
            color_name = "BLACK"
        
        # Create embed with appropriate color based on win/loss
        if result["won"]:
            embed = discord.Embed(
                title="🎲 Deadside Roulette 🎲",
                description="*The wheel of fortune brings a rare smile in the wasteland...*",
                color=WASTELAND_GOLD
            )
        else:
            embed = discord.Embed(
                title="🎲 Deadside Roulette 🎲",
                description="*The wheel of chance shows no mercy in the wasteland...*",
                color=DEADSIDE_DARK
            )
        
        # Add bet information with styling
        bet_header = f"║ 💰 YOUR WAGER: {result['bet_amount']} credits ║"
        bet_type = result['bet_type'].replace('_', ' ').title()
        bet_value = f" {result['bet_value']}" if result['bet_value'] else ""
        bet_display = (
            f"{table_border_top}\n"
            f"║ 🎯 BET: {bet_type}{bet_value} ║\n"
            f"{bet_header}\n"
            f"{table_border_bottom}"
        )
        embed.add_field(name="\u200b", value=bet_display, inline=False)
        
        # Add result display with fancy borders and theming
        result_header = f"║ {color_emoji} RESULT: {spin_result} {color_name} ║"
        result_display = (
            f"{table_border_top}\n"
            f"{result_header}\n"
            f"{table_border_bottom}"
        )
        embed.add_field(name="\u200b", value=result_display, inline=False)
        
        # Set the main image to the result
        embed.set_image(url=result["result_image_url"])
        
        # Win/loss info with enhanced themed messaging
        if result["won"]:
            win_divider = f"═══════ 💎 VICTORY 💎 ═══════"
            win_text = (
                f"{win_divider}\n"
                f"🎉 **FORTUNE FAVORS YOU!** 🎉\n"
                f"You've won **{result['winnings']} credits**\n"
                f"Payout: **{result['payout_multiplier']}:1**\n"
                f"*A rare stroke of luck in the harsh wasteland.*"
            )
            embed.add_field(name="\u200b", value=win_text, inline=False)
            embed.set_footer(text="Your reputation grows in the Deadside...")
        else:
            loss_divider = f"═══════ ☠️ DEFEAT ☠️ ═══════"
            loss_text = (
                f"{loss_divider}\n"
                f"❌ **THE WASTELAND CLAIMS YOUR OFFERING**\n"
                f"You've lost **{result['bet_amount']} credits**\n"
                f"*The unforgiving world of Deadside demands sacrifice.*"
            )
            embed.add_field(name="\u200b", value=loss_text, inline=False)
            embed.set_footer(text="The wasteland is harsh and unforgiving...")
            
        # Show recent history with enhanced styling
        if game.history:
            history_values = []
            for num in game.history:
                if num == 0:
                    history_values.append(f"🟢{num}")
                elif num in game.RED_NUMBERS:
                    history_values.append(f"🔴{num}")
                else:
                    history_values.append(f"⚫{num}")
            
            history_divider = f"═══════ 📜 HISTORY 📜 ═══════"
            history_text = (
                f"{history_divider}\n"
                f"{' '.join(history_values)}\n"
                f"*Patterns in chaos, or mere illusion?*"
            )
            embed.add_field(name="\u200b", value=history_text, inline=False)
            
    elif bet_placed:
        # Show enhanced bet confirmation with Deadside theme
        embed = discord.Embed(
            title="🎲 Deadside Roulette 🎲",
            description="*Your fate now rests with the wheel of fortune...*",
            color=EMERALD_GREEN
        )
        
        # Add a static image of the roulette wheel
        from utils.roulette_svg_generator import get_static_roulette_image
        empty_wheel = get_static_roulette_image(0)  # Default wheel
        embed.set_image(url=empty_wheel)
        
        # Bet details with enhanced display
        bet_type = game.bet_type.title().replace('_', ' ')
        bet_value = f" {game.bet_value}" if game.bet_value else ""
        
        bet_display = (
            f"{table_border_top}\n"
            f"║ 🎯 BET TYPE: {bet_type}{bet_value} ║\n"
            f"║ 💰 WAGER: {game.bet_amount} credits ║\n"
            f"{table_border_bottom}"
        )
        embed.add_field(name="\u200b", value=bet_display, inline=False)
        
        # Add themed spin prompt
        spin_prompt = (
            f"═══════ 🎮 READY ═══════\n"
            f"Click **SPIN** to test your luck in the wasteland.\n"
            f"*The wheel determines who survives and who perishes...*"
        )
        embed.add_field(name="\u200b", value=spin_prompt, inline=False)
        
        # Add Deadside-themed footer
        embed.set_footer(text="Brave the chaos of the Deadside wheel...")
        
    else:
        # Initial roulette view with enhanced styling
        embed = discord.Embed(
            title="🎲 Deadside Roulette 🎲",
            description="*Test your luck in the unforgiving wasteland...*",
            color=EMERALD_GREEN
        )
        
        # Add roulette image
        from utils.roulette_svg_generator import get_static_roulette_image
        empty_wheel = get_static_roulette_image(0)  # Default wheel
        embed.set_image(url=empty_wheel)
        
        # Add themed instructions
        instructions = (
            f"═══════ 📋 RULES ═══════\n"
            f"• Select a bet type from the dropdown\n"
            f"• Set your wager amount\n"
            f"• Spin the wheel to test your fate\n\n"
            f"*Will fortune favor you in the wasteland?*"
        )
        embed.add_field(name="\u200b", value=instructions, inline=False)
        
        # Add available bet types with themed display
        bet_types = (
            f"═══════ 🎯 BET OPTIONS ═══════\n"
            f"🔴 **Red** - Win on any red number (1:1)\n"
            f"⚫ **Black** - Win on any black number (1:1)\n"
            f"🎯 **Straight** - Win on a specific number (35:1)\n"
            f"And many more wasteland wagers..."
        )
        embed.add_field(name="\u200b", value=bet_types, inline=False)
        
        embed.set_footer(text=f"Welcome, {player_name}. The wheel awaits your courage...")
        
    return embed

class RouletteGame:
    """Roulette game implementation with enhanced SVG animations"""
    
    # Roulette wheel numbers (European style with single 0)
    WHEEL_NUMBERS = list(range(0, 37))  # 0-36
    
    # Number colors
    RED_NUMBERS = [1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36]
    BLACK_NUMBERS = [2, 4, 6, 8, 10, 11, 13, 15, 17, 20, 22, 24, 26, 28, 29, 31, 33, 35]
    # 0 is green
    
    # European wheel sequence (clockwise)
    WHEEL_SEQUENCE = [
        0, 32, 15, 19, 4, 21, 2, 25, 17, 34, 6, 27, 13, 36, 11, 
        30, 8, 23, 10, 5, 24, 16, 33, 1, 20, 14, 31, 9, 22, 18, 
        29, 7, 28, 12, 35, 3, 26
    ]
    
    # Bet types and payouts
    BET_TYPES = {
        "straight": {"description": "Single number", "payout": 35},
        "split": {"description": "Two adjacent numbers", "payout": 17},
        "street": {"description": "Three numbers in a row", "payout": 11},
        "corner": {"description": "Four numbers in a square", "payout": 8},
        "six_line": {"description": "Six numbers (two rows)", "payout": 5},
        "column": {"description": "12 numbers (a column)", "payout": 2},
        "dozen": {"description": "12 numbers (1-12, 13-24, 25-36)", "payout": 2},
        "red": {"description": "Red numbers", "payout": 1},
        "black": {"description": "Black numbers", "payout": 1},
        "even": {"description": "Even numbers", "payout": 1},
        "odd": {"description": "Odd numbers", "payout": 1},
        "low": {"description": "Low numbers (1-18)", "payout": 1},
        "high": {"description": "High numbers (19-36)", "payout": 1}
    }
    
    def __init__(self, player_id: str):
        self.player_id = player_id
        self.message = None
        self.bet_amount = 0
        self.bet_type = None
        self.bet_value = None
        self.result = None  # The actual number the wheel landed on
        self.bet_option = None  # Selected bet option
        self.last_result = None  # Full results including win/loss status
        self.history = []  # Last 10 spins
        self._spin_animation_url = None  # Cache for animation URL
        self._result_image_url = None    # Cache for final result image
    
    def place_bet(self, amount: int, bet_type: str, bet_value: Any) -> bool:
        """Place a bet on the roulette table
        
        Args:
            amount: Bet amount
            bet_type: Type of bet (straight, red, black, etc.)
            bet_value: Value to bet on (number or None for color bets)
            
        Returns:
            True if bet is not None was placed successfully, False otherwise
        """
        if bet_type not in self.BET_TYPES:
            return False
            
        self.bet_amount = amount
        self.bet_type = bet_type
        self.bet_value = bet_value
        self.bet_option = bet_value  # Set bet_option as alias for bet_value for compatibility
        
        # Reset animation URLs and result
        self._spin_animation_url = None
        self._result_image_url = None
        self.result = None  # Clear previous result
        
        return True
    
    def spin(self) -> Dict[str, Any]:
        """Spin the roulette wheel
        
        Returns:
            Result data dictionary
        """
        # Randomly select a number
        result = random.choice(self.WHEEL_NUMBERS)
        
        # Store the result number
        self.result = result
        
        # Determine color
        if result == 0:
            color = "green"
        elif result in self.RED_NUMBERS:
            color = "red"
        else:
            color = "black"
            
        # Determine outcome
        won = False
        payout_multiplier = 0
        
        if self.bet_type == "straight":
            won = int(self.bet_value) == result
            payout_multiplier = self.BET_TYPES["straight"]["payout"]
        
        elif self.bet_type == "red":
            won = color == "red"
            payout_multiplier = self.BET_TYPES["red"]["payout"]
            
        elif self.bet_type == "black":
            won = color == "black"
            payout_multiplier = self.BET_TYPES["black"]["payout"]
            
        elif self.bet_type == "even":
            won = result != 0 and result % 2 == 0
            payout_multiplier = self.BET_TYPES["even"]["payout"]
            
        elif self.bet_type == "odd":
            won = result != 0 and result % 2 == 1
            payout_multiplier = self.BET_TYPES["odd"]["payout"]
            
        elif self.bet_type == "low":
            won = 1 <= result <= 18
            payout_multiplier = self.BET_TYPES["low"]["payout"]
            
        elif self.bet_type == "high":
            won = 19 <= result <= 36
            payout_multiplier = self.BET_TYPES["high"]["payout"]
            
        elif self.bet_type == "dozen":
            first_dozen = 1 <= result <= 12
            second_dozen = 13 <= result <= 24
            third_dozen = 25 <= result <= 36
            
            if self.bet_value == "1st" and first_dozen:
                won = True
            elif self.bet_value == "2nd" and second_dozen:
                won = True
            elif self.bet_value == "3rd" and third_dozen:
                won = True
                
            payout_multiplier = self.BET_TYPES["dozen"]["payout"]
            
        elif self.bet_type == "column":
            first_col = result % 3 == 1 and result != 0
            second_col = result % 3 == 2 and result != 0
            third_col = result % 3 == 0 and result != 0
            
            if self.bet_value == "1st" and first_col:
                won = True
            elif self.bet_value == "2nd" and second_col:
                won = True
            elif self.bet_value == "3rd" and third_col:
                won = True
                
            payout_multiplier = self.BET_TYPES["column"]["payout"]
        
        # Update history
        self.history.append(result)
        if len(self.history) > 10:
            self.history = self.history[-10:]
            
        # Calculate winnings
        winnings = 0
        if won:
            winnings = self.bet_amount * payout_multiplier
            
        # Generate animation URL
        from utils.roulette_svg_generator import get_roulette_svg_as_data_url, get_static_roulette_image
        self._spin_animation_url = get_roulette_svg_as_data_url(result)
        self._result_image_url = get_static_roulette_image(result)
        
        # Create result
        self.last_result = {
            "number": result,
            "color": color,
            "won": won,
            "payout_multiplier": payout_multiplier,
            "winnings": winnings,
            "bet_amount": self.bet_amount,
            "bet_type": self.bet_type,
            "bet_value": self.bet_value,
            "net_gain": winnings if won is not None else -self.bet_amount,
            "animation_url": self._spin_animation_url,
            "result_image_url": self._result_image_url
        }
        
        return self.last_result
        
    @property
    def spin_animation_url(self) -> str:
        """Get the URL for the spin animation SVG
        
        Returns:
            Data URL string for the animation
        """
        return self._spin_animation_url if self._spin_animation_url else ""
        
    @property
    def result_image_url(self) -> str:
        """Get the URL for the static result image
        
        Returns:
            Data URL string for the image
        """
        return self._result_image_url if self._result_image_url else ""

class RouletteView(View):
    """Interactive view for roulette game with enhanced Deadside-themed visuals"""
    
    def __init__(self, player_id: str, economy, bet: int = 10):
        super().__init__(timeout=300)  # 5 minutes timeout
        self.player_id = player_id
        self.economy = economy
        self.game = RouletteGame(player_id)
        self.bet = bet
        self.message = None
        self.EMERALD_GREEN = 0x50C878  # Hex color for emerald green
        self.WASTELAND_BROWN = 0x8B4513  # Survival-themed color
        self.DEADSIDE_DARK = 0x2F4F4F  # Dark slate gray for Deadside theme
        self.add_bet_type_select()
        
    def add_bet_type_select(self):
        """Add the bet type selection dropdown"""
        options = [
            SelectOption(label="Red", value="red", description="Bet on red numbers", emoji="🔴"),
            SelectOption(label="Black", value="black", description="Bet on black numbers", emoji="⚫"),
            SelectOption(label="Even", value="even", description="Bet on even numbers", emoji="2️⃣"),
            SelectOption(label="Odd", value="odd", description="Bet on odd numbers", emoji="1️⃣"),
            SelectOption(label="Low (1-18)", value="low", description="Bet on numbers 1-18", emoji="⬇️"),
            SelectOption(label="High (19-36)", value="high", description="Bet on numbers 19-36", emoji="⬆️"),
            SelectOption(label="First Dozen (1-12)", value="dozen:1st", description="Bet on numbers 1-12", emoji="1️⃣"),
            SelectOption(label="Second Dozen (13-24)", value="dozen:2nd", description="Bet on numbers 13-24", emoji="2️⃣"),
            SelectOption(label="Third Dozen (25-36)", value="dozen:3rd", description="Bet on numbers 25-36", emoji="3️⃣"),
            SelectOption(label="Straight (Single Number)", value="straight", description="Bet on a single number", emoji="🎯")
        ]
        
        bet_select = Select(
            placeholder="Select bet type",
            options=options,
            custom_id="bet_type"
        )
        
        bet_select.callback = self.bet_type_selected
        self.add_item(bet_select)
        
    async def on_timeout(self):
        """Handle view timeout by disabling buttons"""
        self.disable_all_items()
        if self.message:
            try:
                embed = discord.Embed(
                    title="🎲 Roulette 🎲",
                    description="Game timed out due to inactivity.",
                    color=discord.Color.dark_gray()
                )
                balance = await self.economy.get_balance()
                embed.add_field(name="Your Balance", value=f"{balance} credits", inline=False)
                await self.message.edit(embed=embed, view=None)
            except Exception as e:
                logger.error(f"Error handling roulette timeout: {e}")
    
    async def bet_type_selected(self, interaction: discord.Interaction):
        """Handle bet type selection"""
        # Check if it's the player's game
        if str(interaction.user.id) != self.player_id:
            await interaction.response.send_message("This isn't your game!", ephemeral=True)
            return
            
        value = interaction.data["values"][0]
        
        # Handle different bet types
        if ":" in value:
            bet_type, bet_value = value.split(":")
        else:
            bet_type = value
            bet_value = None
            
        # For straight bets, we need to show a number input modal
        if bet_type == "straight":
            await interaction.response.send_modal(
                RouletteNumberModal(self, self.bet)
            )
            return
            
        # Place the bet
        self.game.place_bet(self.bet, bet_type, bet_value)
        
        # Create and update embed
        embed = create_advanced_roulette_embed(self.game, bet_placed=True)
        self.disable_all_items()
        
        # Add spin button
        spin_button = Button(style=ButtonStyle.primary, label="Spin", custom_id="spin")
        spin_button.callback = self.spin_wheel
        self.add_item(spin_button)
        
        await interaction.response.edit_message(embed=embed, view=self)
    
    async def spin_wheel(self, interaction: discord.Interaction):
        """Handle spin button click with enhanced emoji-based animations"""
        # Check if it's the player's game
        if str(interaction.user.id) != self.player_id:
            await interaction.response.send_message("This isn't your game!", ephemeral=True)
            return
            
        # Check if player is not None has enough credits
        balance = await self.economy.get_balance()
        if balance < self.game.bet_amount:
            await interaction.response.send_message(
                f"You don't have enough credits! You need {self.game.bet_amount} credits to place this bet.",
                ephemeral=True
            )
            return
            
        # Show spinning animation
        await interaction.response.defer()
        
        # Get animation frames from our new emoji-based generator
        from utils.roulette_svg_generator import get_spin_animation_frame, generate_result_display, get_static_roulette_image
        
        # Create a loading embed for the spin animation with Deadside theme
        loading_embed = discord.Embed(
            title="🎲 Deadside Roulette 🎲",
            description="*The wheel begins to spin as tension builds...*",
            color=self.DEADSIDE_DARK
        )
        
        loading_embed.add_field(
            name="Your Bet",
            value=f"**{self.game.bet_amount}** credits on **{self.game.bet_type}**",
            inline=False
        )
        
        # Add initial animation frame
        loading_embed.add_field(
            name="Wheel Status",
            value=get_spin_animation_frame(0),
            inline=False
        )
        
        loading_embed.set_footer(text="The wasteland holds its breath as fate decides...")
        
        await interaction.followup.edit_message(
            message_id=self.message.id,
            embed=loading_embed,
            view=None
        )
            
        # Remove the bet amount
        await self.economy.remove_currency(self.game.bet_amount, "roulette_bet", {
            "game": "roulette", 
            "bet_type": self.game.bet_type
        })
        
        # Enhanced multi-frame spinning animation sequence
        for frame_idx in range(5):  # Multiple frames for dynamic animation
            spin_frame = frame_idx % 3  # We have 3 different frames, cycle through them
            
            # Progressive messages for the animation sequence
            spin_messages = [
                "*The wheel spins with deadly precision...*",
                "*The ball bounces wildly across the pockets...*",
                "*Metal clatters against metal as the wheel turns...*",
                "*The wheel begins to slow, the ball still dancing...*",
                "*The ball is about to find its home...*"
            ]
            
            # Update the embed with the current animation frame
            spin_embed = discord.Embed(
                title="🎲 Deadside Roulette 🎲",
                description=spin_messages[frame_idx],
                color=0x4169E1 if frame_idx < 3 else self.WASTELAND_BROWN  # Change color as it slows
            )
            
            spin_embed.add_field(
                name="Your Bet",
                value=f"**{self.game.bet_amount}** credits on **{self.game.bet_type}**",
                inline=False
            )
            
            # Add the current animation frame
            spin_embed.add_field(
                name="Wheel Status",
                value=get_spin_animation_frame((spin_frame + frame_idx) % 3),  # Cycle through frames
                inline=False
            )
            
            # Dynamic footer for immersion
            footer_texts = [
                "The wasteland holds many secrets...",
                "Fortune hangs in the balance...",
                "Will luck favor you in these ruins?",
                "The ball skips between the numbers...",
                "The moment of truth approaches..."
            ]
            spin_embed.set_footer(text=footer_texts[frame_idx])
            
            # Update the message with each animation frame
            await interaction.followup.edit_message(
                message_id=self.message.id,
                embed=spin_embed,
                view=None
            )
            
            # Decreasing delay as we get closer to the result for dramatic effect
            await asyncio.sleep(1.0 if frame_idx < 2 else 0.8)
        
        # Spin the wheel and determine the result
        result = self.game.spin()
        
        # Final result embed with emerald-themed styling
        is_win = result["won"]
        result_embed = discord.Embed(
            title="🎲 Deadside Roulette 🎲",
            description=f"The ball lands on **{result['number']}**!",
            color=self.EMERALD_GREEN if is_win is not None else self.WASTELAND_BROWN
        )
        
        # Add the visual result display using our fancy emoji-based display
        if self.game.result is not None:
            result_embed.add_field(
                name="Final Result",
                value=generate_result_display(self.game.result),
                inline=False
            )
            
            # Show the wheel position with the result highlighted
            result_embed.add_field(
                name="Wheel Position",
                value=get_static_roulette_image(self.game.result),
                inline=False
            )
        else:
            # Fallback if result is None
            result_embed.add_field(
                name="Final Result",
                value="*The wheel appears to have malfunctioned...*",
                inline=False
            )
        
        # Add bet information with enhanced formatting
        result_embed.add_field(
            name="Your Wager",
            value=f"**{self.game.bet_amount}** credits on **{self.game.bet_type}**",
            inline=False
        )
        
        # Update player economy and create themed outcome message
        if is_win is not None:
            winnings = result["winnings"]
            
            # Create an exciting win message with Deadside theme
            if self.game.bet_type == "straight" and self.game.bet_option is None and self.game.result is None and int(self.game.bet_option) == self.game.result:
                win_message = f"💎 **JACKPOT!** 💎\nYour precise prediction paid off with **{winnings}** credits!"
            else:
                win_message = f"**WINNER!**\nYou've claimed **{winnings}** credits from the wasteland!"
            
            result_embed.add_field(
                name="💰 PAYOUT 💰",
                value=win_message,
                inline=False
            )
            
            # Process the economy updates
            await self.economy.add_currency(
                winnings,
                "roulette_win",
                {"game": "roulette", "bet_type": self.game.bet_type}
            )
            await self.economy.update_gambling_stats("roulette", True, winnings)
        else:
            # Create a themed but encouraging loss message
            result_embed.add_field(
                name="💀 LOSS 💀",
                value=f"The wheel shows no mercy. You've lost **{self.game.bet_amount}** credits to the wasteland.",
                inline=False
            )
            
            await self.economy.update_gambling_stats("roulette", False, self.game.bet_amount)
        
        # Show updated balance
        new_balance = await self.economy.get_balance()
        result_embed.add_field(
            name="Your Balance", 
            value=f"**{new_balance}** credits", 
            inline=False
        )
        
        # Add thematic footer
        if is_win is not None:
            result_embed.set_footer(text="Fortune favors the brave in Deadside!")
        else:
            result_embed.set_footer(text="Even the greatest survivors face defeat in the wasteland...")
        
        # Add play again button with themed label
        self.clear_items()
        play_again = Button(
            style=ButtonStyle.success, 
            label="Try Your Luck Again", 
            custom_id="play_again",
            emoji="🎲"
        )
        play_again.callback = self.play_again
        self.add_item(play_again)
        
        # Add quit button
        quit_button = Button(
            style=ButtonStyle.secondary,
            label="Leave Table", 
            custom_id="quit_roulette",
            emoji="🚶"
        )
        quit_button.callback = self.quit_button
        self.add_item(quit_button)
        
        # Update the message with final result
        await interaction.followup.edit_message(
            message_id=self.message.id,
            embed=result_embed,
            view=self
        )
    
    async def play_again(self, interaction: discord.Interaction):
        """Handle play again button click"""
        # Check if it's the player's game
        if str(interaction.user.id) != self.player_id:
            await interaction.response.send_message("This isn't your game!", ephemeral=True)
            return
            
        # Reset game but keep history
        history = self.game.history
        self.game = RouletteGame(self.player_id)
        self.game.history = history
        
        # Clear and add items
        self.clear_items()
        self.add_bet_type_select()
        
        # Create embed
        embed = create_advanced_roulette_embed(self.game)
        
        await interaction.response.edit_message(embed=embed, view=self)
        
    async def quit_button(self, interaction: discord.Interaction):
        """Handle quit button click"""
        # Check if it's the player's game
        if str(interaction.user.id) != self.player_id:
            await interaction.response.send_message("This isn't your game!", ephemeral=True)
            return
            
        # Get balance
        balance = await self.economy.get_balance()
        
        # Border styling elements
        table_border_top = "╔═════════════════════════════╗"
        table_border_bottom = "╚═════════════════════════════╝"
        
        # Create goodbye embed with enhanced styling
        embed = discord.Embed(
            title="🎲 Deadside Roulette 🎲",
            description="*You step away from the wheel, perhaps wiser for the experience...*",
            color=self.EMERALD_GREEN
        )
        
        # Add balance info with styling
        balance_display = (
            f"{table_border_top}\n"
            f"║ 💰 CREDITS: {balance} ║\n"
            f"{table_border_bottom}"
        )
        embed.add_field(name="\u200b", value=balance_display, inline=False)
        
        # Add history if available is not None
        if self.game.history:
            history_values = []
            for num in self.game.history:
                if num == 0:
                    history_values.append(f"🟢{num}")
                elif num in self.game.RED_NUMBERS:
                    history_values.append(f"🔴{num}")
                else:
                    history_values.append(f"⚫{num}")
            
            history_divider = "═══════ 📜 SESSION HISTORY 📜 ═══════"
            history_text = (
                f"{history_divider}\n"
                f"{' '.join(history_values)}\n"
                f"*The patterns of fortune and fate are etched in your memory.*"
            )
            embed.add_field(name="\u200b", value=history_text, inline=False)
        
        embed.set_footer(text="The wasteland will be waiting when you return...")
        
        # Clear items
        self.clear_items()
        
        await interaction.response.edit_message(embed=embed, view=None)
    
    def disable_all_items(self):
        """Disable all buttons and selects"""
        for item in self.children:
            item.disabled = True

class RouletteNumberModal(discord.ui.Modal):
    """Modal for entering a number for straight bets"""
    
    def __init__(self, view: Optional[RouletteView] = None, bet: int = 10):
        super().__init__(title="Choose Your Fate (0-36)")
        self._parent_view = None
        self.bet = bet
        
        # Add number input with themed label
        self.number_input = discord.ui.TextInput(
            label="Enter a number between 0 and 36",
            placeholder="Enter a single number (e.g. 17)",
            min_length=1,
            max_length=2,
            required=True
        )
        self.add_item(self.number_input)
        
        # If view is provided during initialization, set it
        if view is not None:
            self.set_parent_view(view)
            
    @property
    def view(self):
        """Get the parent view"""
        return self._parent_view
        
    def set_parent_view(self, view):
        """Set the parent view with type checking"""
        # Set the parent view 
        self._parent_view = view
        
    async def on_submit(self, interaction: discord.Interaction):
        """Handle form submission"""
        try:
            number = int(self.number_input.value)
            
            # Check if parent is not None view is set
            if self is None.view:
                await interaction.response.send_message(
                    "Error: Unable to process your bet. Please try again.",
                    ephemeral=True
                )
                return
                
            # Validate number range
            if 0 <= number <= 36:
                # Place the bet (with null check)
                if hasattr(self.view, 'game') and self.view.game:
                    self.view.game.place_bet(self.bet, "straight", number)
                    
                    # Create bet confirmation with enhanced themed styling
                    # Use create_roulette_embed since create_advanced_roulette_embed has different parameters
                    player_name = interaction.user.display_name if interaction is not None else "Wanderer"
                    embed = create_roulette_embed(self.view.game, bet_placed=True)
                    
                    # Make sure view has required methods
                    if hasattr(self.view, 'disable_all_items'):
                        self.view.disable_all_items()
                    
                    # Add spin button with danger style for excitement
                    spin_button = Button(style=ButtonStyle.danger, label="Spin", emoji="🎲", custom_id="spin")
                    
                    # Make sure spin_wheel exists
                    if hasattr(self.view, 'spin_wheel'):
                        spin_button.callback = self.view.spin_wheel
                        
                    # Make sure add_item exists
                    if hasattr(self.view, 'add_item'):
                        self.view.add_item(spin_button)
                    
                    await interaction.response.edit_message(embed=embed, view=self.view)
                else:
                    await interaction.response.send_message(
                        "Error: Game not initialized. Please try again.",
                        ephemeral=True
                    )
            else:
                await interaction.response.send_message(
                    "Please enter a valid number between 0 and 36!",
                    ephemeral=True
                )
        except ValueError:
            await interaction.response.send_message(
                "Please enter a valid number!",
                ephemeral=True
            )
        except Exception as e:
            # Catch any other errors to prevent crashes
            logger.error(f"Error in RouletteNumberModal.on_submit: {e}")
            await interaction.response.send_message(
                "An error occurred. Please try again.",
                ephemeral=True
            )

def create_roulette_embed(game: Optional[RouletteGame] = None, bet_placed: bool = False, spin_result: bool = False, animation: bool = True) -> discord.Embed:
    """Create an embed for roulette game with SVG animations
    
    Args:
        game: Roulette game instance, or None for a default display
        bet_placed: Whether a bet has been placed
        spin_result: Whether to show spin results
        animation: Whether to show animation (True) or static result (False)
        
    Returns:
        Embed object
    """
    # Handle null game by creating a default instance
    if game is None:
        logger.warning("create_roulette_embed called with null game, using default instance")
        game = RouletteGame("default")
    # Theming
    EMERALD_GREEN = 0x50C878
    DEADSIDE_RED = 0xA91101
    WASTELAND_GOLD = 0xD4AF37
    DARK_METAL = 0x333333
    
    if spin_result is not None and game.last_result:
        # Show spin results
        result = game.last_result
        
        # Determine color based on result
        if result["color"] == "red":
            embed_color = DEADSIDE_RED
            number_display = f"🔴 {result['number']}"
        elif result["color"] == "black":
            embed_color = DARK_METAL
            number_display = f"⚫ {result['number']}"
        else:  # Green for 0
            embed_color = EMERALD_GREEN
            number_display = f"🟢 {result['number']}"
            
        embed = discord.Embed(
            title="🎲 Deadside Roulette 🎲",
            description=f"The ball lands on {number_display}!",
            color=embed_color
        )
        
        # Set the roulette wheel image
        if animation is not None and "animation_url" in result:
            # Show animated spinning wheel
            embed.set_image(url=result["animation_url"])
        elif "result_image_url" in result:
            # Show static result image
            embed.set_image(url=result["result_image_url"])
        
        # Bet details
        bet_display = f"{result['bet_type'].title()}"
        if result['bet_value'] is not None:
            if result['bet_type'] == 'straight':
                bet_display += f" ({result['bet_value']})"
            else:
                bet_display += f" ({result['bet_value']})"
                
        embed.add_field(
            name="Your Wager",
            value=f"{bet_display}: {result['bet_amount']} credits",
            inline=False
        )
        
        # Win/loss info with themed messaging
        if result["won"]:
            win_text = f"🎉 **VICTORY!** You won {result['winnings']} credits! 🎉"
            win_text += f"\nPayout: {result['payout_multiplier']}:1"
            embed.add_field(name="Result", value=win_text, inline=False)
            embed.set_footer(text="Your luck in the wasteland pays off. Word of your fortune spreads...")
        else:
            embed.add_field(
                name="Result", 
                value="❌ **DEFEATED!** The wasteland claims your credits!", 
                inline=False
            )
            embed.set_footer(text="The wasteland is harsh and unforgiving...")
            
        # Show recent history with styling
        if game.history:
            history_values = []
            for num in game.history:
                if num == 0:
                    history_values.append(f"🟢{num}")
                elif num in game.RED_NUMBERS:
                    history_values.append(f"🔴{num}")
                else:
                    history_values.append(f"⚫{num}")
            
            history_text = " ".join(history_values)
            embed.add_field(name="Recent Spins", value=history_text, inline=False)
            
    elif bet_placed is not None:
        # Show bet confirmation
        embed = discord.Embed(
            title="🎲 Deadside Roulette 🎲",
            description="Your bet has been placed! Are you feeling lucky in the wasteland?",
            color=WASTELAND_GOLD
        )
        
        # Add a static image of an empty roulette wheel
        from utils.roulette_svg_generator import get_static_roulette_image
        empty_wheel = get_static_roulette_image(0)  # Just use 0 for a default wheel
        embed.set_image(url=empty_wheel)
        
        # Bet details
        bet_display = f"{game.bet_type.title()}"
        if game.bet_value is not None:
            if game.bet_type == 'straight':
                bet_display += f" ({game.bet_value})"
            else:
                bet_display += f" ({game.bet_value})"
                
        embed.add_field(
            name="Your Wager",
            value=f"{bet_display}: {game.bet_amount} credits",
            inline=False
        )
        
        embed.add_field(
            name="Ready?",
            value="The croupier awaits your signal. Click 'Spin' to set destiny in motion!",
            inline=False
        )
        
        # Show recent history with styling
        if game.history:
            history_values = []
            for num in game.history:
                if num == 0:
                    history_values.append(f"🟢{num}")
                elif num in game.RED_NUMBERS:
                    history_values.append(f"🔴{num}")
                else:
                    history_values.append(f"⚫{num}")
            
            history_text = " ".join(history_values)
            embed.add_field(name="Recent Spins", value=history_text, inline=False)
        
        embed.set_footer(text="Tension builds as you prepare to test your luck...")
            
    else:
        # Initial embed
        embed = discord.Embed(
            title="🎲 Deadside Roulette 🎲",
            description=f"Place your wager: {game.bet_amount} credits",
            color=EMERALD_GREEN
        )
        
        # Add a static image of an empty roulette wheel
        from utils.roulette_svg_generator import get_static_roulette_image
        empty_wheel = get_static_roulette_image(0)  # Just use 0 for a default wheel
        embed.set_image(url=empty_wheel)
        
        embed.add_field(
            name="How to Play",
            value="In the dangerous wastes, gambling is a way of life. Select your bet type from the dropdown menu below.",
            inline=False
        )
        
        # Add bet types info
        bet_info = (
            "**Bet Types:**\n"
            "• **Red/Black**: 1:1 payout\n"
            "• **Even/Odd**: 1:1 payout\n"
            "• **Low/High**: 1:1 payout\n"
            "• **Dozens**: 2:1 payout\n"
            "• **Straight**: 35:1 payout"
        )
        embed.add_field(name="Odds", value=bet_info, inline=False)
        
        # Show recent history with styling
        if game.history:
            history_values = []
            for num in game.history:
                if num == 0:
                    history_values.append(f"🟢{num}")
                elif num in game.RED_NUMBERS:
                    history_values.append(f"🔴{num}")
                else:
                    history_values.append(f"⚫{num}")
            
            history_text = " ".join(history_values)
            embed.add_field(name="Recent Spins", value=history_text, inline=False)
        
        embed.set_footer(text="Will fortune favor you in the wasteland?")
            
    return embed

def create_slots_embed(slot_machine=None, bet=10, symbols=None, winnings=None, spinning=False, spin_frame=None, player_name="Wanderer"):
    """
    Create a themed slots embed with consistent styling 
    matching the other gambling games
    
    Args:
        slot_machine: SlotMachine instance, or None for a default instance
        bet: Current bet amount
        symbols: List of symbol names if this is not None is a result display
        winnings: Amount won (if any)
        spinning: Whether this is a spinning animation frame
        spin_frame: Which frame of spinning animation to show
        player_name: Name of the player
        
    Returns:
        discord.Embed: Styled embed for current slot state
    """
    # Create a default slot machine if none is not None is provided
    if slot_machine is None:
        logger.warning("create_slots_embed called with null slot_machine, using default instance")
        slot_machine = SlotMachine()
    # Theme colors
    EMERALD_GREEN = 0x50C878  # Emerald green for Deadside theme
    WASTELAND_GOLD = 0xD4AF37  # Gold for win highlights
    DEADSIDE_DARK = 0x2F4F4F  # Dark slate gray for background
    
    # Border styling elements (matching blackjack and roulette)
    table_border_top = "╔═════════════════════════════╗"
    table_border_bottom = "╚═════════════════════════════╝"
    divider = "═══════════════════════════════"
    
    if symbols is not None and not spinning:
        # Results display
        won = winnings is not None and winnings > 0
        multiplier = winnings // bet if won is not None and bet else 0
        
        # Create appropriate themed embed based on outcome
        if won is not None:
            if multiplier >= 20:
                description = "*Extraordinary fortune! The salvage machine yields rare treasure!*"
            elif multiplier >= 10:
                description = "*The salvage machine buzzes with extraordinary success!*"
            else:
                description = "*The salvage machine found something of value in the wasteland debris.*"
                
            embed = discord.Embed(
                title="🎮 Deadside Salvage Machine 🎮",
                description=description,
                color=WASTELAND_GOLD
            )
        else:
            embed = discord.Embed(
                title="🎮 Deadside Salvage Machine 🎮",
                description="*The salvage machine whirs and clicks, but finds no valuables.*",
                color=DEADSIDE_DARK
            )
        
        # Add bet information with styled border
        bet_display = (
            f"{table_border_top}\n"
            f"║ 💰 INVESTMENT: {bet} credits ║\n"
            f"{table_border_bottom}"
        )
        embed.add_field(name="\u200b", value=bet_display, inline=False)
        
        # Add slot display with styled border
        # Get display emojis for each symbol
        display_symbols = []
        for symbol in symbols:
            if isinstance(symbol, str) and symbol in slot_machine.symbol_emojis:
                display_symbols.append(slot_machine.symbol_emojis[symbol])
            else:
                display_symbols.append("❓")
        
        slot_display = (
            f"{table_border_top}\n"
            f"║  {display_symbols[0]}  ║  {display_symbols[1]}  ║  {display_symbols[2]}  ║\n"
            f"{table_border_bottom}"
        )
        embed.add_field(name="\u200b", value=slot_display, inline=False)
        
        # Add result information with appropriate styling
        if won is not None:
            # Create a win display with appropriate excitement level
            if multiplier >= 20:
                win_divider = f"═══════ 💎 MEGA JACKPOT 💎 ═══════"
            elif multiplier >= 10:
                win_divider = f"═══════ 💰 JACKPOT 💰 ═══════"
            else:
                win_divider = f"═══════ ✨ SALVAGE REWARD ✨ ═══════"
                
            win_text = (
                f"{win_divider}\n"
                f"🎉 **RESOURCES RECOVERED!** 🎉\n"
                f"You've earned **{winnings} credits**\n"
                f"Multiplier: **{multiplier}x**\n"
                f"*A valuable discovery in the harsh wasteland.*"
            )
            embed.add_field(name="\u200b", value=win_text, inline=False)
            
            # Add item descriptions in styled format
            item_descriptions = []
            for symbol in set(symbols):  # Use set to avoid duplicates
                if symbol in slot_machine.symbol_descriptions:
                    item_descriptions.append(f"• **{slot_machine.symbol_names[symbol]}**: {slot_machine.symbol_descriptions[symbol]}")
            
            if item_descriptions is not None:
                item_divider = f"═══════ 📦 ITEMS FOUND 📦 ═══════"
                items_text = (
                    f"{item_divider}\n"
                    f"{chr(10).join(item_descriptions)}\n"
                    f"*These items could save your life in Deadside.*"
                )
                embed.add_field(name="\u200b", value=items_text, inline=False)
                
            embed.set_footer(text="Your reputation as a skilled scavenger grows...")
        else:
            # Loss message
            loss_divider = f"═══════ ☠️ SALVAGE FAILED ☠️ ═══════"
            loss_text = (
                f"{loss_divider}\n"
                f"❌ **NO VALUABLES FOUND**\n"
                f"You've lost **{bet} credits**\n"
                f"*The unforgiving wasteland yields nothing this time.*"
            )
            embed.add_field(name="\u200b", value=loss_text, inline=False)
            embed.set_footer(text="The wastelands are often barren of valuable resources...")
        
    elif spinning is None and spin_frame is None:
        # Spinning animation frame
        animation_messages = [
            "*Gears turning as the machine processes debris...*",
            "*Mechanical arms sorting through wasteland artifacts...*",
            "*Salvage processor analyzing potential resources...*",
            "*Calibrating detection systems for valuable items...*",
            "*Machine vibrates as it scans for usable materials...*"
        ]
        
        embed = discord.Embed(
            title="🎮 Deadside Salvage Machine 🎮",
            description=animation_messages[min(spin_frame, len(animation_messages)-1)],
            color=DEADSIDE_DARK
        )
        
        # Add the bet info
        bet_display = (
            f"{table_border_top}\n"
            f"║ 💰 INVESTMENT: {bet} credits ║\n"
            f"{table_border_bottom}"
        )
        embed.add_field(name="\u200b", value=bet_display, inline=False)
        
        # Create a random animation frame for the spinning reels
        random_symbols = []
        for _ in range(3):
            random_symbols.append(random.choice(slot_machine.symbols))
            
        # Use the actual animation frame
        # Get display emojis for each symbol
        display_symbols = []
        for symbol in random_symbols:
            if isinstance(symbol, str) and symbol in slot_machine.symbol_emojis:
                display_symbols.append(slot_machine.symbol_emojis[symbol])
            else:
                display_symbols.append("🔄")
        
        slot_display = (
            f"{table_border_top}\n"
            f"║  {display_symbols[0]}  ║  {display_symbols[1]}  ║  {display_symbols[2]}  ║\n"
            f"{table_border_bottom}"
        )
        embed.add_field(name="\u200b", value=slot_display, inline=False)
        
        # Footer with suspenseful message
        footer_texts = [
            "Scanning for valuable salvage...",
            "Processing wasteland materials...",
            "Filtering through the debris...",
            "Analyzing detected resources...",
            "Calculating salvage probabilities..."
        ]
        embed.set_footer(text=footer_texts[min(spin_frame, len(footer_texts)-1)])
        
    else:
        # Initial view - no spin, no results
        embed = discord.Embed(
            title="🎮 Deadside Salvage Machine 🎮",
            description="*An automated device that processes wasteland debris for useful materials.*",
            color=EMERALD_GREEN
        )
        
        # Add the bet info
        bet_display = (
            f"{table_border_top}\n"
            f"║ 💰 INVESTMENT: {bet} credits ║\n"
            f"{table_border_bottom}"
        )
        embed.add_field(name="\u200b", value=bet_display, inline=False)
        
        # Add themed instructions
        instructions = (
            f"═══════ 📋 OPERATION GUIDE ═══════\n"
            f"• Insert credits to power the salvage machine\n"
            f"• Press **SPIN** to process wasteland debris\n"
            f"• Similar resources increase payout value\n"
            f"• Special combinations yield bonus rewards\n\n"
            f"*Will you discover valuable resources in the wasteland?*"
        )
        embed.add_field(name="\u200b", value=instructions, inline=False)
        
        # Add potential rewards info
        rewards = (
            f"═══════ 💎 RARE FINDS ═══════\n"
            f"🟢 **Emerald**: Most valuable resource (25x)\n"
            f"🔵 **Dogtags**: Fallen survivor's ID (15x)\n"
            f"🟡 **Helmet**: Critical protection (8x)\n"
            f"And many more wasteland resources..."
        )
        embed.add_field(name="\u200b", value=rewards, inline=False)
        
        embed.set_footer(text=f"Welcome, {player_name}. What will you salvage from the ruins?")
        
    return embed

class SlotMachine:
    def __init__(self):
        from utils.svg_loader import get_svg_symbol_data, DEADSIDE_EMOJIS
        
        # Load symbols data
        symbol_data = get_svg_symbol_data()
        
        # Initialize symbol collections
        self.symbols = [item[0] for item in symbol_data]  # Symbol names
        self.symbol_names = {item[0]: item[1] for item in symbol_data}  # Display names
        self.symbol_descriptions = {item[0]: item[2] for item in symbol_data}  # Descriptions
        
        # Emoji representation of each symbol
        self.symbol_emojis = DEADSIDE_EMOJIS
        
        # Define weights for each symbol (rarer items have lower chance)
        self.weights = [
            20,  # pistol - common
            18,  # rifle - common
            16,  # medkit - uncommon
            15,  # ammo - uncommon
            13,  # backpack - uncommon
            10,  # food - uncommon
            8,   # helmet - rare
            5,   # dogtag - rare
            3    # emerald - legendary
        ]
        
        # Define payouts based on rarity and value
        self.payouts = {
            "pistol": 2,     # Common weapon
            "rifle": 4,      # Better weapon
            "medkit": 6,     # Valuable healing
            "ammo": 3,       # Necessary supply
            "backpack": 5,   # Storage upgrade
            "food": 4,       # Survival essential
            "helmet": 8,     # Rare protection
            "dogtag": 15,    # Valuable collectible
            "emerald": 25    # Ultimate jackpot item
        }
        
        # Special combinations with higher payouts
        self.special_combos = {
            ("emerald", "emerald", "emerald"): 50,  # Triple Emeralds (mega jackpot)
            ("dogtag", "dogtag", "dogtag"): 30,     # Triple Dogtags
            ("pistol", "rifle", "ammo"): 10,        # Loadout combo
            ("medkit", "food", "backpack"): 12,     # Survival kit combo
            ("helmet", "dogtag", "emerald"): 20     # Elite combo
        }
        
        # Enhanced animation frames for spinning effect with Deadside theme
        self.spin_frames = [
            ["🔄", "⚙️", "🔄"],
            ["⚙️", "🔄", "⚙️"],
            ["🔫", "🪖", "💎"],
            ["🔄", "💎", "🔄"],
            ["⚙️", "🔫", "⚙️"],
            ["🎲", "🎰", "🎲"],
            ["🎮", "🕹️", "🎮"]
        ]
        
        # Stylized borders for the slot machine display with emerald accents
        self.top_border = "╔══💎══╦══💎══╦══💎══╗"
        self.mid_border = "╠══💎══╬══💎══╬══💎══╣"
        self.bottom_border = "╚══💎══╩══💎══╩══💎══╝"
    
    def format_slot_display(self, symbols):
        """Format a nice-looking slot display with borders
        
        Args:
            symbols: List of symbol names
            
        Returns:
            Formatted text representation of the slot display
        """
        # Get display emojis for each symbol (with fallback) from our collection
        display_symbols = []
        for symbol in symbols:
            if isinstance(symbol, str) and symbol in self.symbol_emojis:
                display_symbols.append(self.symbol_emojis[symbol])
            else:
                display_symbols.append("❓")
        
        # Format into a stylized slot display with emerald-themed borders
        lines = [
            self.top_border,
            f"║  {display_symbols[0]}  ║  {display_symbols[1]}  ║  {display_symbols[2]}  ║",
            self.bottom_border
        ]
        return "\n".join(lines)
    
    def get_random_spin_frame(self):
        """Get a random frame for spinning animation"""
        symbols = []
        for _ in range(3):
            symbols.append(random.choice(self.symbols))
        return symbols
    
    def get_symbol_description(self, symbol):
        """Get the description of a symbol"""
        if symbol in self.symbol_descriptions:
            return f"{self.symbol_names[symbol]}: {self.symbol_descriptions[symbol]}"
        return "Unknown item"
    
    def get_symbol_image_url(self, symbol):
        """Get the image URL for a symbol"""
        from utils.svg_loader import get_svg_as_data_url
        return get_svg_as_data_url(symbol)
    
    def get_symbol_name(self, symbol):
        """Get the display name for a symbol"""
        return self.symbol_names.get(symbol, "Unknown")
    
    def spin(self) -> Tuple[List[str], int]:
        """Spin the slot machine and return results
        
        Returns:
            Tuple of symbol names and win multiplier
        """
        # Select symbols based on weights
        results = random.choices(self.symbols, weights=self.weights, k=3)
        
        # Check for special combinations
        tuple_result = tuple(results)
        if tuple_result in self.special_combos:
            multiplier = self.special_combos[tuple_result]
        # Check if all is not None symbols are the same
        elif results[0] == results[1] == results[2]:
            multiplier = self.payouts[results[0]]
        # Check if two is not None symbols are the same
        elif results[0] == results[1] or results[0] == results[2] or results[1] == results[2]:
            # Find the duplicated symbol
            if results[0] == results[1]:
                symbol = results[0]
            elif results[0] == results[2]:
                symbol = results[0]
            else:
                symbol = results[1]
            multiplier = self.payouts[symbol] // 2  # Half payout for two matching
        else:
            multiplier = 0
        
        return results, multiplier

class SlotsView(View):
    def __init__(self, player_id: str, economy, bet: int = 10):
        super().__init__(timeout=300)  # 5 minutes timeout
        self.player_id = player_id
        self.economy = economy
        self.slot_machine = SlotMachine()
        self.bet = bet
        self.message = None
        
    async def on_timeout(self):
        """Handle view timeout by disabling buttons"""
        self.disable_all_buttons()
        if self.message:
            try:
                # Get player's balance for the timeout message
                balance = await self.economy.get_balance()
                
                # Use our standardized embed for timeout state
                embed = create_slots_embed(
                    self.slot_machine, 
                    bet=self.bet
                )
                
                # Update embed for timeout state
                embed.description = "*The salvage machine powers down due to inactivity.*"
                embed.add_field(name="Credits Balance", value=f"{balance} credits", inline=True)
                embed.set_footer(text="Return to the wasteland when you're ready to try again.")
                
                await self.message.edit(embed=embed, view=None)
            except Exception as e:
                logger.error(f"Error handling slots timeout: {e}")
    
    @discord.ui.button(label="Spin", style=ButtonStyle.success, emoji="🔄")
    async def spin_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Check if it's the player's game
        if str(interaction.user.id) != self.player_id:
            await interaction.response.send_message("This isn't your salvage machine!", ephemeral=True)
            return
        
        # Check if player is not None has enough credits
        balance = await self.economy.get_balance()
        if balance < self.bet:
            await interaction.response.send_message(
                f"You don't have enough credits! You need {self.bet} credits to operate this machine.", 
                ephemeral=True
            )
            return
        
        # Remove the bet amount
        await self.economy.remove_currency(self.bet, "slots_bet")
        
        # Spin the slots
        symbols, multiplier = self.slot_machine.spin()
        
        # Calculate winnings
        winnings = self.bet * multiplier
        won = winnings > 0
        
        # Update player economy and gambling stats
        if won is not None:
            await self.economy.add_currency(winnings, "slots_win", {"game": "slots", "multiplier": multiplier})
            await self.economy.update_gambling_stats("slots", True, winnings)
        else:
            await self.economy.update_gambling_stats("slots", False, self.bet)
        
        # Defer response to allow for animation sequence
        await interaction.response.defer()
        
        # Get the player's name for personalization
        player_name = interaction.user.display_name
        
        # Initial loading message
        loading_embed = create_slots_embed(
            self.slot_machine,
            bet=self.bet,
            spinning=True,
            spin_frame=0,
            player_name=player_name
        )
        loading_message = await interaction.followup.send(embed=loading_embed)
        
        # Animation sequence showing spinning reels
        for frame in range(5):
            temp_embed = create_slots_embed(
                self.slot_machine,
                bet=self.bet,
                spinning=True,
                spin_frame=frame,
                player_name=player_name
            )
            await loading_message.edit(embed=temp_embed)
            await asyncio.sleep(0.6)
        
        # More advanced animation showing individual reels stopping one by one
        # Spin each reel with its own animation
        for reel in range(3):
            # Generate displays for this stage of spinning
            for frame in range(2):
                # Generate partial results
                current_display = []
                
                for i in range(3):
                    if i < reel:  # This reel has already stopped
                        current_display.append(symbols[i])
                    else:  # This reel is still spinning
                        current_display.append(random.choice(self.slot_machine.symbols))
                
                # Update with standardized embed
                temp_embed = create_slots_embed(
                    self.slot_machine,
                    bet=self.bet,
                    symbols=current_display,
                    spinning=True,
                    spin_frame=frame + reel,
                    player_name=player_name
                )
                
                await loading_message.edit(embed=temp_embed)
                await asyncio.sleep(0.5)
                
            # Show this reel stopping
            current_display = []
            for i in range(3):
                if i <= reel:  # This reel and previous reels have stopped
                    current_display.append(symbols[i])
                else:  # These reels are still spinning
                    current_display.append(random.choice(self.slot_machine.symbols))
                    
            temp_embed = create_slots_embed(
                self.slot_machine,
                bet=self.bet,
                symbols=current_display,
                spinning=True,
                spin_frame=4,  # Use last frame for dramatic effect
                player_name=player_name
            )
            
            await loading_message.edit(embed=temp_embed)
            await asyncio.sleep(0.7)  # Slightly longer pause when a reel stops
        
        # Show the final result with our standardized embed
        new_balance = await self.economy.get_balance()
        
        final_embed = create_slots_embed(
            self.slot_machine,
            bet=self.bet,
            symbols=symbols,
            winnings=winnings,
            player_name=player_name
        )
        
        # Add balance information
        balance_change = new_balance - balance
        balance_indicator = "+" if balance_change > 0 else ""
        
        # Add this as an inline field to maintain our styling
        final_embed.add_field(
            name="Credits Balance", 
            value=f"{new_balance} credits ({balance_indicator}{balance_change})",
            inline=True
        )
        
        # Add special thumbnail for jackpot wins
        if won is not None and multiplier >= 20:
            # Use emerald as thumbnail for big wins regardless of actual symbols
            emerald_url = self.slot_machine.get_symbol_image_url("emerald")
            if emerald_url is not None:
                final_embed.set_thumbnail(url=emerald_url)
        
        await loading_message.edit(embed=final_embed)
        self.message = loading_message
    
    @discord.ui.button(label="Change Bet", style=ButtonStyle.primary, emoji="💰")
    async def change_bet_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Check if it's the player's game
        if str(interaction.user.id) != self.player_id:
            await interaction.response.send_message("This isn't your salvage machine!", ephemeral=True)
            return
        
        # Create a modal for the bet amount with parent_view set during initialization
        modal = BetModal("Set Salvage Investment", self.bet, parent_view=self)
        
        # Show the modal to the user
        await interaction.response.send_modal(modal)
    
    @discord.ui.button(label="Quit", style=ButtonStyle.danger, emoji="🚪")
    async def quit_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Check if it's the player's game
        if str(interaction.user.id) != self.player_id:
            await interaction.response.send_message("This isn't your salvage machine!", ephemeral=True)
            return
        
        # Disable all buttons
        self.disable_all_buttons()
        
        # Get player's balance
        balance = await self.economy.get_balance()
        player_name = interaction.user.display_name
        
        # Create final embed using our standardized design
        embed = create_slots_embed(
            self.slot_machine, 
            bet=self.bet,
            player_name=player_name
        )
        
        # Update for quit state
        embed.description = "*Salvage operations terminated. Machine shutting down.*"
        embed.add_field(name="Credits Balance", value=f"{balance} credits", inline=True)
        embed.set_footer(text="Thank you for using the Emergency Resource Acquisition Device.")
        
        # Update the message with the disabled view
        await interaction.response.edit_message(embed=embed, view=self)
        
        # Store message for cleanup
        self.message = interaction.message
    
    def disable_all_buttons(self):
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True

class BetModal(discord.ui.Modal):
    def __init__(self, title: str, current_bet: int, parent_view=None):
        super().__init__(title=title)
        # Initialize with typing that matches SlotsView
        self._parent_view = None
        
        # Add the bet input field
        self.bet_input = discord.ui.TextInput(
            label="Enter your bet amount",
            placeholder=f"Current bet: {current_bet} credits",
            default=str(current_bet),
            required=True,
            min_length=1,
            max_length=6
        )
        self.add_item(self.bet_input)
        
        # If parent_view is provided during initialization, set it
        if parent_view is not None:
            self.set_parent_view(parent_view)
            
    @property
    def parent_view(self):
        """Get the parent view"""
        return self._parent_view
        
    def set_parent_view(self, view):
        """Set the parent view with type checking"""
        # Set the parent view 
        self._parent_view = view
    
    async def on_submit(self, interaction: discord.Interaction):
        """Process the submitted bet amount"""
        try:
            # Convert input to integer and validate
            new_bet = int(self.bet_input.value)
            
            if new_bet <= 0:
                await interaction.response.send_message("Bet must be greater than 0 credits.", ephemeral=True)
                return
                
            # Check if player is not None has enough credits for this bet
            if self.parent_view and self.parent_view.economy:
                balance = await self.parent_view.economy.get_balance()
                if new_bet > balance:
                    await interaction.response.send_message(
                        f"You don't have enough credits for that bet. Your balance: {balance} credits", 
                        ephemeral=True
                    )
                    return
            
            # Update the bet in the parent view
            if self.parent_view:
                old_bet = self.parent_view.bet
                self.parent_view.bet = new_bet
                
                # Create a new embed with updated bet amount
                player_name = interaction.user.display_name
                embed = create_slots_embed(
                    self.parent_view.slot_machine,
                    bet=new_bet,
                    player_name=player_name
                )
                
                # Add message about bet change
                embed.add_field(
                    name="Bet Updated", 
                    value=f"Investment changed from {old_bet} to {new_bet} credits", 
                    inline=False
                )
                
                # Update the message with new embed
                await interaction.response.edit_message(embed=embed, view=self.parent_view)
            else:
                # Fallback in case parent_view is not set
                await interaction.response.send_message(f"Bet set to {new_bet} credits", ephemeral=True)
                
        except ValueError:
            # Not a valid number
            await interaction.response.send_message("Please enter a valid number for your bet", ephemeral=True)
        except Exception as e:
            logger.error(f"Error processing bet input: {e}")
            await interaction.response.send_message("There was an error processing your bet", ephemeral=True)