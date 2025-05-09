"""
Asset Generator for High-Quality Gambling Game Elements

This script uses OpenAI's DALL-E to generate photorealistic assets for:
1. Playing cards with Deadside-themed design
2. Roulette wheel with highly detailed Deadside helmet elements
3. Slot machine symbols with Deadside survival theme
"""

import os
import base64
import requests
import json
import logging
from io import BytesIO
from PIL import Image

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Make sure we have the API key
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY environment variable is not set.")

# Define API endpoint
DALLE_API_URL = "https://api.openai.com/v1/images/generations"

# Set up the headers for API requests
headers = {
    "Authorization": f"Bearer {OPENAI_API_KEY}",
    "Content-Type": "application/json"
}

def generate_image(prompt, size="1024x1024", quality="hd", n=1):
    """Generate an image using DALL-E 3"""
    try:
        payload = {
            "model": "dall-e-3",
            "prompt": prompt,
            "n": n,
            "size": size,
            "quality": quality
        }
        
        logger.info(f"Generating image with prompt: {prompt[:50]}...")
        response = requests.post(DALLE_API_URL, headers=headers, json=payload)
        response_data = response.json()
        
        if 'error' in response_data:
            logger.error(f"Error: {response_data['error']['message']}")
            return None
        
        if 'data' in response_data and len(response_data['data']) > 0:
            return response_data['data'][0]['url']
        else:
            logger.error("No image data returned")
            return None
    
    except Exception as e:
        logger.error(f"Error generating image: {str(e)}")
        return None

def save_image(url, filename):
    """Download and save an image from a URL"""
    try:
        response = requests.get(url)
        if response.status_code == 200:
            image = Image.open(BytesIO(response.content))
            
            # Create directory if it doesn\'t exist
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            
            # Save image
            image.save(filename)
            logger.info(f"Saved image to {filename}")
            return True
        else:
            logger.error(f"Failed to download image: HTTP {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"Error saving image: {str(e)}")
        return False

def generate_card_assets():
    """Generate high-quality card designs with Deadside theme"""
    # Define card prompts
    card_prompts = {
        "card_back": """Hyper-realistic playing card back design in 4K resolution. Post-apocalyptic Deadside theme. 
        Centered metal combat helmet with glowing green eyes, spiked outer rim, battle-damaged metal texture. 
        Dark background with emerald green accents. Industrial worn metal card border. 
        Extremely detailed, photorealistic rendering with ray-traced lighting and reflections.
        The card should have sharp edges, consistent size ratio, high-end game asset quality.""",
        
        "ace_spades": """Hyper-realistic playing card Ace of Spades in 4K resolution. Post-apocalyptic Deadside theme. 
        Large spade symbol with metal texture and battle damage. Top left and bottom right corners show "A" and spade symbol.
        Background features faded combat helmet with glowing green eyes. High-contrast design with emerald green accents.
        Extremely detailed, photorealistic rendering with ray-traced lighting and reflections. Gaming asset quality.""",
        
        "king_hearts": """Hyper-realistic playing card King of Hearts in 4K resolution. Post-apocalyptic Deadside theme.
        Large red heart symbol with metal texture and battle damage effects. Top left and bottom right corners show "K" and heart symbol.
        Background features faded combat helmet with glowing green eyes. High-contrast design with emerald green accents.
        Extremely detailed, photorealistic rendering with ray-traced lighting and reflections. Gaming asset quality."""
    }
    
    # Generate and save card images
    output_dir = "static/realistic_cards"
    for card_name, prompt in card_prompts.items():
        image_url = generate_image(prompt)
        if image_url:
            save_image(image_url, f"{output_dir}/{card_name}.png")

def generate_roulette_assets():
    """Generate high-quality roulette wheel design with Deadside theme"""
    roulette_prompt = """Hyper-realistic top-down view of a roulette wheel in 4K resolution. Post-apocalyptic Deadside theme.
    The wheel has alternating red and black numbered pockets (0-36), with a green pocket for zero.
    The outer rim features metal combat helmet designs with glowing green eyes repeated around the circumference.
    Battle-damaged metal textures, industrial feel, with emerald green accents and highlights.
    Intricate details on the central hub showing Deadside helmet emblem.
    Extremely detailed, photorealistic rendering with ray-traced lighting, reflections and weathered metal textures.
    Professional gaming asset quality with cinematic lighting."""
    
    output_dir = "static/realistic_roulette"
    image_url = generate_image(roulette_prompt, size="1024x1024", quality="hd")
    if image_url:
        save_image(image_url, f"{output_dir}/roulette_wheel.png")

def generate_slot_symbols():
    """Generate high-quality slot machine symbols with Deadside theme"""
    # Define slot symbol prompts with industrial post-apocalyptic style
    symbol_prompts = {
        "emerald": """Hyper-realistic emerald gemstone on dark background in 4K resolution. Post-apocalyptic Deadside theme.
        The emerald has a vibrant green glow, faceted surface with light reflections. Square format, isolated object.
        Extremely detailed, photorealistic rendering with ray-traced lighting. Professional gaming asset quality.""",
        
        "pistol": """Hyper-realistic weathered pistol on dark background in 4K resolution. Post-apocalyptic Deadside theme.
        Battle-damaged metal handgun with scratches and wear. Square format, isolated object.
        Extremely detailed, photorealistic rendering with ray-traced lighting. Professional gaming asset quality.""",
        
        "medkit": """Hyper-realistic medical kit on dark background in 4K resolution. Post-apocalyptic Deadside theme.
        Weathered metal box with red cross, battle damage and scratches. Square format, isolated object.
        Extremely detailed, photorealistic rendering with ray-traced lighting. Professional gaming asset quality.""",
        
        "helmet": """Hyper-realistic combat helmet on dark background in 4K resolution. Post-apocalyptic Deadside theme.
        Metal helmet with glowing green eyes/visor, spiked rim, battle damage. Square format, isolated object.
        Extremely detailed, photorealistic rendering with ray-traced lighting. Professional gaming asset quality.""",
        
        "dogtag": """Hyper-realistic military dog tags on dark background in 4K resolution. Post-apocalyptic Deadside theme.
        Weathered metal tags on chain with engraved text, battle damage. Square format, isolated object.
        Extremely detailed, photorealistic rendering with ray-traced lighting. Professional gaming asset quality."""
    }
    
    # Generate and save slot symbols
    output_dir = "static/realistic_slots"
    for symbol_name, prompt in symbol_prompts.items():
        image_url = generate_image(prompt, size="1024x1024", quality="hd")
        if image_url:
            save_image(image_url, f"{output_dir}/{symbol_name}.png")

if __name__ == "__main__":
    # Check if we're missing the OpenAI API key
    if not OPENAI_API_KEY:
        print("ERROR: Missing OPENAI_API_KEY environment variable")
        print("Please set this variable before running the script.")
        exit(1)
        
    # Create output directories
    os.makedirs("static/realistic_cards", exist_ok=True)
    os.makedirs("static/realistic_roulette", exist_ok=True)
    os.makedirs("static/realistic_slots", exist_ok=True)
    
    # Generate all assets
    print("Generating high-quality gambling assets...")
    
    print("\n1. Generating card designs...")
    generate_card_assets()
    
    print("\n2. Generating roulette wheel...")
    generate_roulette_assets()
    
    print("\n3. Generating slot machine symbols...")
    generate_slot_symbols()
    
    print("\nAsset generation complete!")