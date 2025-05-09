import asyncio
import motor.motor_asyncio

async def main():
    db = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017')['emeralds_killfeed']
    kill_count = await db.kills.count_documents({})
    print(f'Total kill events in database: {kill_count}')
    
    # Get the latest 5 kills if any exist
    if kill_count > 0:
        latest_kills = await db.kills.find().sort([('timestamp', -1)]).limit(5).to_list(5)
        print("\nLatest 5 kill events:")
        for kill in latest_kills:
            print(f"Killer: {kill.get('killer_name')} -> Victim: {kill.get('victim_name')} - Weapon: {kill.get('weapon')} - Time: {kill.get('timestamp')}")

if __name__ == "__main__":
    asyncio.run(main())