import asyncio
from TikTokLive import TikTokLiveClient
from TikTokLive.events import GiftEvent
from TikTokLive.client.errors import UserNotFoundError

# Temporary test creator
CREATOR_USERNAME = "joshbrissenden_"

async def run_listener():
    while True:
        try:
            print(f"Starting listener for {CREATOR_USERNAME}...")

            client = TikTokLiveClient(unique_id=CREATOR_USERNAME)

            @client.on(GiftEvent)
            async def on_gift(event: GiftEvent):
                diamond_value = event.gift.info.diamond_count
                total_diamonds = diamond_value * event.repeat_count

                print("\n--- Gift Received ---")
                print("Creator:", CREATOR_USERNAME)
                print("From:", event.user.nickname)
                print("Gift:", event.gift.name)
                print("Diamonds per item:", diamond_value)
                print("Count:", event.repeat_count)
                print("Total diamonds:", total_diamonds)
                print("----------------------\n")

            await client._connect()   # Low-level connect
            await client._join_room() # Join live feed

            print("Connected. Listening for gifts...")
            await client._listen()    # Start receiving events

        except UserNotFoundError:
            print("UserNotFoundError: TikTok returned invalid data. Retrying in 10 seconds...")
            await asyncio.sleep(10)

        except Exception as e:
            print(f"Error: {e}. Restarting in 10 seconds...")
            await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(run_listener())
