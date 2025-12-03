import os
import asyncio
from TikTokLive import TikTokLiveClient
from TikTokLive.events import GiftEvent
from TikTokLive.client.errors import UserNotFoundError

CREATOR_USERNAME = "harryyoung_7"

async def run_listener():
    while True:
        try:
            print(f"Starting listener for {CREATOR_USERNAME}...")

            # Correct cookie injection for TikTokLive 6.6.5
            client = TikTokLiveClient(
                unique_id=CREATOR_USERNAME,
                headers={
                    "Cookie": f"sessionid={os.getenv('TIKTOK_SESSIONID')}"
                }
            )

            @client.on(GiftEvent)
            async def on_gift(event: GiftEvent):
                # Universal diamond extraction
                diamond_value = None

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
                print("From:", event.user.nickname)
                print("Gift:", event.gift.name)
                print("Diamonds per item:", diamond_value)
                print("Count:", event.repeat_count)
                print("Total diamonds:", total)
                print("----------------------\n")

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
