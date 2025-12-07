import os
import asyncio
import psycopg2
import requests
from TikTokLive import TikTokLiveClient
from TikTokLive.events import GiftEvent

DATABASE_URL = os.getenv("DATABASE_URL")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

DIAMOND_ALERT_THRESHOLD = 4999

# GitHub raw folder
GITHUB_BASE = "https://raw.githubusercontent.com/richpow/tiktok-live-listener/main/gifts"

# How aggressive the scanner is
SCAN_DELAY_BETWEEN_CREATORS = 0.2      # seconds between checks of individual users
SCAN_SLEEP_BETWEEN_FULL_PASSES = 10    # pause after a full pass over all creators

# Active live listeners
ACTIVE_CLIENTS = {}  # username -> TikTokLiveClient


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
                total_diamonds,
            ),
        )

        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        print("Database insert error:", e)


def format_number(n):
    return f"{n:,}"


def build_gift_image_url(gift_name):
    key = gift_name.lower().strip().replace(" ", "_").replace("'", "")
    url = f"{GITHUB_BASE}/{key}.png"
    try:
        check = requests.get(url, timeout=3)
        if check.status_code == 200:
            return url
    except Exception as e:
        print("Gift image fetch error:", e)
    return None


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
        return

    description_text = (
        f"**{creator_username}** has just received a **{gift_name}** from **{sender_username}**"
    )

    embed = {
        "title": "Gift Alert",
        "description": description_text,
        "color": 3447003,
        "fields": [
            {
                "name": "Creator",
                "value": creator_username,
                "inline": True
            },
            {
                "name": "Sent By",
                "value": f"{sender_username} | {sender_display_name}",
                "inline": True
            },
            {
                "name": "Diamonds",
                "value": format_number(total_diamonds),
                "inline": False
            }
        ]
    }

    if gift_image_url:
        embed["thumbnail"] = {"url": gift_image_url}

    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"embeds": [embed]}, timeout=3)
    except Exception as e:
        print("Discord error:", e)


async def start_listener_for_creator(creator_username: str):
    """
    Start a long lived TikTokLiveClient for a creator that is confirmed live.
    This keeps running until the live ends or an error disconnects the client.
    """
    if creator_username in ACTIVE_CLIENTS:
        return  # already listening

    client = TikTokLiveClient(unique_id=creator_username)

    # Keep library logs quiet to avoid Railway log spam
    try:
        client.logger.setLevel("WARNING")
    except Exception:
        pass

    # Mark this creator as active
    ACTIVE_CLIENTS[creator_username] = client
    print(f"Started recording gifts for {creator_username}")
    print(f"Currently recording gifts for {len(ACTIVE_CLIENTS)} users live right now")

    @client.on(GiftEvent)
    async def on_gift(event: GiftEvent):
        sender_username = event.user.unique_id
        sender_display_name = event.user.nickname
        gift_name = event.gift.name

        diamond_value = (
            getattr(event.gift, "diamond_count", None)
            or getattr(event.gift, "diamond_value", None)
            or 0
        )

        repeat_count = event.repeat_count
        total_diamonds = diamond_value * repeat_count

        # Always log to Neon
        log_gift_to_neon(
            creator_username,
            sender_username,
            sender_display_name,
            gift_name,
            diamond_value,
            repeat_count,
            total_diamonds,
        )

        # Send Discord alert only for bigger gifts
        if total_diamonds >= DIAMOND_ALERT_THRESHOLD:
            gift_image = build_gift_image_url(gift_name)
            send_discord_alert(
                creator_username,
                sender_username,
                sender_display_name,
                gift_name,
                diamond_value,
                repeat_count,
                total_diamonds,
                gift_image_url=gift_image,
            )

    async def runner():
        try:
            await client.connect()
            await client.listen()
        except Exception as e:
            print(f"Listener error for {creator_username}: {e}")
        finally:
            # Remove from active when live ends or fails
            if creator_username in ACTIVE_CLIENTS:
                ACTIVE_CLIENTS.pop(creator_username, None)
                print(f"Stopped recording gifts for {creator_username}, user is now offline")
                print(f"Currently recording gifts for {len(ACTIVE_CLIENTS)} users live right now")

    # Fire and forget
    asyncio.create_task(runner())


async def scan_creators_loop(creators):
    """
    Loop forever:
      iterate over all creators
      skip anyone already being listened to
      use is_live to see who just went live
      start a listener for them
    """
    while True:
        for username in creators:
            # If we are already tracking this creator while they are live, skip
            if username in ACTIVE_CLIENTS:
                await asyncio.sleep(SCAN_DELAY_BETWEEN_CREATORS)
                continue

            try:
                # Lightweight check, much cheaper than full websocket
                probe_client = TikTokLiveClient(unique_id=username)

                try:
                    # Keep logs low on the probe client as well
                    try:
                        probe_client.logger.setLevel("WARNING")
                    except Exception:
                        pass

                    is_live = await probe_client.is_live()
                finally:
                    # Close underlying HTTP client if possible
                    try:
                        await probe_client.disconnect()
                    except Exception:
                        pass

                if is_live:
                    await start_listener_for_creator(username)

            except Exception as e:
                # Only print scan errors that might matter
                print(f"Scan error for {username}: {e}")

            # Small pause between checks to avoid hammering TikTok and Railway
            await asyncio.sleep(SCAN_DELAY_BETWEEN_CREATORS)

        # Short pause after a full pass
        await asyncio.sleep(SCAN_SLEEP_BETWEEN_FULL_PASSES)


async def main():
    creators = load_creators()
    # Single scanner loop, individual live listeners spin off as needed
    await scan_creators_loop(creators)


if __name__ == "__main__":
    asyncio.run(main())
