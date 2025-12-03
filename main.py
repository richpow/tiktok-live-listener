import asyncio
from TikTokLive import TikTokLiveClient
from TikTokLive.events import GiftEvent

CREATOR_USERNAME = "stargirl09_"   # or whichever worked

async def run_listener():
    while True:
        try:
            print(f"Starting listener for {CREATOR_USERNAME}")

            client = TikTokLiveClient(unique_id=CREATOR_USERNAME)

            @client.on(GiftEvent)
            async def on_gift(event: GiftEvent):

                diamond_value = None

                # Handle all possible diamond fields safely
                if hasattr(event.gift, "diamond_count"):
                    diamond_value = event.gift.diamond_count
                elif hasattr(event.gift, "diamond_value"):
                    diamond_value = event.gift.diamond_value
                elif hasattr(event.gift, "info") and hasattr(event.gift.info, "diamond_count"):
                    diamond_value = event.gift.info.diamond_count

                if diamond_value is None:
                    diamond_value = 0

                total = diamond_value * event.repeat_count

                print("\n--- Gift Received ---")
                print("Creator:", CREATOR_USERNAME)
                print("From:", event.user.unique_id)         # ← USERNAME
                print("Display Name:", event.user.nickname)  # optional
                print("Gift:", event.gift.name)
                print("Diamonds per item:", diamond_value)
                print("Count:", event.repeat_count)
                print("Total diamonds:", total)
                print("----------------------\n")

            await client.connect()
            print("Connected. Listening for gifts")
            await client.listen()

        except Exception as e:
            print(f"Error: {e}. Restarting in 10 seconds")
            await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(run_listener())
