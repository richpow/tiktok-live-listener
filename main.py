import asyncio
from TikTokLive import TikTokLiveClient
from TikTokLive.events import GiftEvent
from TikTokLive.client.errors import UserNotFoundError

CREATOR_USERNAME = "harryyoung_7"

async def run_listener():
    while True:
        try:
            print(f"Starting listener for {CREATOR_USERNAME}...")

            client = TikTokLiveClient(unique_id=CREATOR_USERNAME)

            @client.on(GiftEvent)
            async def on_gift(event: GiftEvent):

                # Universal safe diamond extraction
                diamond_value = None

                # 1. Newer format
                if hasattr(event.gift, "diamond_count"):
                    diamond_value = event.gift.diamond_count

                # 2. Older format
                elif hasattr(event.gift, "diamond_value"):
                    diamond_value = event.gift.diamond_value

                # 3. Some accounts return nested info
                elif hasattr(event.gift, "info") and hasattr(event.gift.info, "diamond_count"):
                    diamond_value = event.gift.info.diamond_count

                # 4. Fallback for free gifts or unknown fields
                if diamond_value is None:
                    diamond_value = 0

                total = diamond_value * event.repeat_count

                print("\n--- Gift Received ---")
                print("Creator:", CREATOR_USERNAME)
                print("From:", event.user.nickname)
                print("Gift:", event.gift.name)
                print("Diamonds per item:", diamond_value)
                print("Count:", event.repeat_count)
                print("Total diamonds:", total)
                print("----------------------\n")

            # Connect with official TikTokLive methods
            await client.connect()
            print("Connected. Listening for gifts...")
            await client.listen()

        except UserNotFoundError:
            print("UserNotFoundError: Retrying in 10 seconds...")
            await asyncio.sleep(10)

        except Exception as e:
            print(f"Error: {e}. Restarting in 10 seconds...")
            await asyncio.sleep(10)


if __name__ == "__main__":
    asyncio.run(run_listener())
