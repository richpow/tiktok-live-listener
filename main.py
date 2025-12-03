from TikTokLive import TikTokLiveClient
from TikTokLive.events import GiftEvent

# Temporary test creator (currently live)
CREATOR_USERNAME = "joshbrissenden_"

client = TikTokLiveClient(unique_id=CREATOR_USERNAME)

@client.on(GiftEvent)
async def on_gift(event: GiftEvent):
    print("\n--- Gift Received ---")
    print("Creator:", CREATOR_USERNAME)
    print("From:", event.user.nickname)
    print("Gift:", event.gift.name)
    print("Diamonds per item:", event.gift.diamond_value)
    print("Count:", event.repeat_count)
    print("Total diamonds:", event.gift.diamond_value * event.repeat_count)
    print("----------------------\n")

if __name__ == "__main__":
    print("Starting TikTok live listener…")
    client.run()
