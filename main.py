import os
import asyncio
import psycopg2
import requests
from TikTokLive import TikTokLiveClient
from TikTokLive.events import GiftEvent

DATABASE_URL = os.getenv("DATABASE_URL")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

DIAMOND_ALERT_THRESHOLD = 3000

# StreamToEarn gift API for GB
GIFT_API_URL = "https://streamtoearn.io/api/v1/gifts?region=GB"

# Global lookup: gift name (lowercased) -> image url
GIFT_LOOKUP = {}


def get_db():
    return psycopg2.connect(DATABASE_URL)


def load_creators():
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        select tiktok_username
        from users
        where tiktok_username is not null
          and tiktok_username <> ''
          and creator_id is not null
          and creator_id <> ''
        """
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    creators = [r[0] for r in rows]
    print(f"Loaded {len(creators)} creators from users")
    return creators


def load_gift_lookup():
    lookup = {}
    try:
        print("Fetching gift data from StreamToEarn")
        resp = requests.get(GIFT_API_URL, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if isinstance(data, dict):
            if "gifts" in data and isinstance(data["gifts"], list):
                items = data["gifts"]
            elif "data" in data and isinstance(data["data"], list):
                items = data["data"]
            else:
                items = []
        elif isinstance(data, list):
            items = data
        else:
            items = []

        count = 0
        for item in items:
            if not isinstance(item, dict):
                continue

            name = item.get("name") or item.get("title") or item.get("gift_name")
            if not name:
                continue

            image_url = (
                item.get("image_url")
                or item.get("image")
                or item.get("icon")
                or item.get("icon_url")
            )

            if image_url:
                lookup[name.lower()] = image_url
                count += 1

        print(f"Built gift lookup with {count} entries")

    except Exception as e:
        print("Failed to load gift lookup from StreamToEarn:", e)

    return lookup


def log_gift_to_neon(
    creator_username,
    sender_username,
    sender_display_name,
    gift_name,
    diamonds_per_item,
    repeat_count,
    total_diamonds
):
    try:
        conn = get_db()
        cur = conn.cursor()

        cur.execute(
            """
            insert into fasttrack_live_gifts
            (creator_username,
             sender_username,
             sender_display_name,
             gift_name,
             diamonds_per_item,
             repeat_count,
             total_diamonds)
            values (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                creator_username,
                sender_username,
                sender_display_name,
                gift_name,
                diamonds_per_item,
                repeat_count,
                total_diamonds
            ),
        )

        conn.commit()
        cur.close()
        conn.close()
        print(f"Gift logged in Neon for {creator_username}")

    except Exception as e:
        print("Database insert error:", e)


def send_discord_alert(
    creator_username,
    sender_username,
    sender_display_name,
    sender_level,
    gift_name,
    diamonds_per_item,
    repeat_count,
    total_diamonds,
    gift_image_url=None
):
    if not DISCORD_WEBHOOK_URL:
        print("No webhook URL found")
        return

    description_text = f"**{creator_username}** has just received a Gift **{gift_name}**"

    embed = {
        "title": "Gift Alert",
        "description": description_text,
        "color": 65280,
        "fields": [
            {"name": "Creator", "value": creator_username, "inline": False},
            {
                "name": "From",
                "value": f"{sender_username} (Level {sender_level})",
                "inline": False
            },
            {"name": "Display Name", "value": sender_display_name, "inline": False},
            {"name": "Gift", "value": gift_name, "inline": False},
            {"name": "Diamonds per item", "value": str(diamonds_per_item), "inline": True},
            {"name": "Count", "value": str(repeat_count), "inline": True},
            {"name": "Total diamonds", "value": str(total_diamonds), "inline": False}
        ]
    }

    if gift_image_url:
        embed["thumbnail"] = {"url": gift_image_url}

    payload = {"embeds": [embed]}

    try:
        requests.post(DISCORD_WEBHOOK_URL, json=payload)
        print("Discord alert sent")
    except Exception as e:
        print("Discord alert error:", e)


async def run_listener_for_creator(creator_username):
    global GIFT_LOOKUP

    while True:
        try:
            print(f"Starting listener for {creator_username}")
            client = TikTokLiveClient(unique_id=creator_username)

            @client.on(GiftEvent)
            async def on_gift(event: GiftEvent):
                sender_username = event.user.unique_id
                sender_display_name = event.user.nickname

                sender_level = getattr(event.user, "level", None)
                if sender_level is None:
                    sender_level = "Unknown"

                gift_name = event.gift.name

                diamond_value = None
                if hasattr(event.gift, "diamond_count"):
                    diamond_value = event.gift.diamond_count
                elif hasattr(event.gift, "diamond_value"):
                    diamond_value = event.gift.diamond_value
                elif hasattr(event.gift, "info") and hasattr(event.gift.info, "diamond_count"):
                    diamond_value = event.gift.info.diamond_count

                if diamond_value is None:
                    diamond_value = 0

                repeat_count = event.repeat_count
                total_diamonds = diamond_value * repeat_count

                print("\n--- Gift Received ---")
                print("Creator:", creator_username)
                print("From username:", sender_username)
                print("Sender level:", sender_level)
                print("Display name:", sender_display_name)
                print("Gift:", gift_name)
                print("Diamonds per item:", diamond_value)
                print("Count:", repeat_count)
                print("Total diamonds:", total_diamonds)
                print("----------------------\n")

                log_gift_to_neon(
                    creator_username,
                    sender_username,
                    sender_display_name,
                    gift_name,
                    diamond_value,
                    repeat_count,
                    total_diamonds
                )

                gift_key = gift_name.lower().strip()
                gift_image_url = GIFT_LOOKUP.get(gift_key)

                if total_diamonds >= DIAMOND_ALERT_THRESHOLD:
                    send_discord_alert(
                        creator_username,
                        sender_username,
                        sender_display_name,
                        sender_level,
                        gift_name,
                        diamond_value,
                        repeat_count,
                        total_diamonds,
                        gift_image_url=gift_image_url
                    )

            await client.connect()
            print(f"Connected. Listening for gifts for {creator_username}")
            await client.listen()

        except Exception as e:
            msg = str(e)
            offline_like = (
                "UserNotFoundError" in msg
                or "Expecting value: line 1 column 1" in msg
                or "SIGN_NOT_200" in msg
            )

            if offline_like:
                print(
                    f"Creator {creator_username} probably not live or TikTok returned bad data. Retrying this creator in 1800 seconds"
                )
                await asyncio.sleep(1800)
            else:
                print(
                    f"Error in listener for {creator_username}: {e}. Restarting this creator in 10 seconds"
                )
                await asyncio.sleep(10)


async def main():
    global GIFT_LOOKUP

    GIFT_LOOKUP = load_gift_lookup()

    creators = load_creators()

    if not creators:
        print("No creators found with tiktok_username and creator_id")
        return

    tasks = []
    for username in creators:
        tasks.append(asyncio.create_task(run_listener_for_creator(username)))

    print(f"Started {len(tasks)} listener tasks")
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
