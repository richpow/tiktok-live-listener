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
    print(f"Loaded {len(creators)} creators")
    return creators


def load_gift_lookup():
    lookup = {}
    try:
        print("Loading gift images list…")
        resp = requests.get(GIFT_API_URL, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if isinstance(data, dict):
            items = data.get("gifts") or data.get("data") or []
        elif isinstance(data, list):
            items = data
        else:
            items = []

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

        print(f"Gift lookup loaded with {len(lookup)} images")

    except Exception as e:
        print("Failed to load gift image list:", e)

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

    except Exception as e:
        print("Database insert error:", e)


def send_discord_alert(
    creator_username,
    sender_username,
    sender_display_name,
    gift_name,
    diamonds_per_item,
    repeat_count,
    total_diamonds,
    gift_image_url=None
):
    if not DISCORD_WEBHOOK_URL:
        print("Webhook missing")
        return

    diamonds_fmt = f"{diamonds_per_item:,}"
    total_fmt = f"{total_diamonds:,}"

    description_text = f"**{creator_username}** has just received a **{gift_name}**"

    embed = {
        "title": "Gift Alert",
        "description": description_text,
        "color": 65280,
        "fields": [
            {"name": "Creator", "value": creator_username, "inline": False},
            {"name": "From", "value": sender_username, "inline": False},
            {"name": "Display Name", "value": sender_display_name, "inline": False},
            {"name": "Gift", "value": gift_name, "inline": False},
            {"name": "Diamonds", "value": diamonds_fmt, "inline": True},
            {"name": "Count", "value": str(repeat_count), "inline": True},
            {"name": "Total diamonds", "value": total_fmt, "inline": False}
        ]
    }

    # Small gift icon top right
    if gift_image_url:
        embed["author"] = {"name": " ", "icon_url": gift_image_url}

    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"embeds": [embed]})
        print(f"Alert sent for {creator_username}: {gift_name} ({total_fmt})")
    except Exception as e:
        print("Alert send error:", e)


async def run_listener_for_creator(creator_username):
    global GIFT_LOOKUP

    while True:
        try:
            print(f"Listener started: {creator_username}")
            client = TikTokLiveClient(unique_id=creator_username)

            @client.on(GiftEvent)
            async def on_gift(event: GiftEvent):
                sender_username = event.user.unique_id
                sender_display_name = event.user.nickname
                gift_name = event.gift.name

                # diamond extraction
                if hasattr(event.gift, "diamond_count"):
                    diamonds_per_item = event.gift.diamond_count
                elif hasattr(event.gift, "diamond_value"):
                    diamonds_per_item = event.gift.diamond_value
                elif hasattr(event.gift, "info") and hasattr(event.gift.info, "diamond_count"):
                    diamonds_per_item = event.gift.info.diamond_count
                else:
                    diamonds_per_item = 0

                repeat_count = event.repeat_count
                total_diamonds = diamonds_per_item * repeat_count

                # store in Neon
                log_gift_to_neon(
                    creator_username,
                    sender_username,
                    sender_display_name,
                    gift_name,
                    diamonds_per_item,
                    repeat_count,
                    total_diamonds
                )

                # lookup gift image
                gift_key = gift_name.lower().strip()
                gift_image_url = GIFT_LOOKUP.get(gift_key)

                if total_diamonds >= DIAMOND_ALERT_THRESHOLD:
                    send_discord_alert(
                        creator_username,
                        sender_username,
                        sender_display_name,
                        gift_name,
                        diamonds_per_item,
                        repeat_count,
                        total_diamonds,
                        gift_image_url
                    )

            await client.connect()
            await client.listen()

        except Exception as e:
            message = str(e)

            # Offline or TikTok error
            offline_error = (
                "UserNotFoundError" in message
                or "SIGN_NOT_200" in message
                or "Expecting value" in message
            )

            if offline_error:
                # quiet retry
                await asyncio.sleep(1800)
            else:
                print(f"Error for {creator_username}: {e}")
                await asyncio.sleep(10)


async def main():
    global GIFT_LOOKUP

    GIFT_LOOKUP = load_gift_lookup()
    creators = load_creators()

    if not creators:
        print("No creators found")
        return

    tasks = [asyncio.create_task(run_listener_for_creator(u)) for u in creators]

    print(f"Started {len(tasks)} listeners")
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
