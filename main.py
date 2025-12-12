import os
import asyncio
import time
import psycopg2
import requests
from TikTokLive import TikTokLiveClient
from TikTokLive.events import GiftEvent

DATABASE_URL = os.getenv("DATABASE_URL")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

DIAMOND_ALERT_THRESHOLD = 9999
GITHUB_BASE = "https://raw.githubusercontent.com/richpow/tiktok-live-listener/main/gifts"

SCAN_DELAY_BETWEEN_CREATORS = 0.2
SCAN_SLEEP_BETWEEN_FULL_PASSES = 10

MAX_RUNTIME_SECONDS = 60 * 60 * 3
STALL_TIMEOUT_SECONDS = 60 * 8

ACTIVE_CLIENTS = {}
SERVICE_START_TIME = time.time()
LAST_ACTIVITY_TIME = time.time()


def touch_activity():
    global LAST_ACTIVITY_TIME
    LAST_ACTIVITY_TIME = time.time()


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
    except:
        pass


def build_gift_image_url(gift_name):
    key = gift_name.lower().strip().replace(" ", "_").replace("'", "")
    url = f"{GITHUB_BASE}/{key}.png"
    try:
        resp = requests.get(url, timeout=3)
        if resp.status_code == 200:
            return url
    except:
        pass
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

    embed = {
        "title": "Gift Alert",
        "description": f"{creator_username} received {gift_name} from {sender_username}",
        "color": 3447003,
        "fields": [
            {"name": "Creator", "value": creator_username, "inline": True},
            {"name": "Sent By", "value": f"{sender_username} | {sender_display_name}", "inline": True},
            {"name": "Diamonds", "value": f"{total_diamonds:,}"},
        ],
    }

    if gift_image_url:
        embed["thumbnail"] = {"url": gift_image_url}

    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"embeds": [embed]}, timeout=3)
    except:
        pass


async def start_listener_for_creator(creator_username):
    if creator_username in ACTIVE_CLIENTS:
        return

    client = TikTokLiveClient(unique_id=creator_username)
    ACTIVE_CLIENTS[creator_username] = client

    print(f"Started listener for {creator_username}")
    print(f"Active listeners {len(ACTIVE_CLIENTS)}")

    @client.on(GiftEvent)
    async def on_gift(event):
        touch_activity()

        sender_username = event.user.unique_id
        sender_display_name = event.user.nickname
        gift_name = event.gift.name

        diamond_value = (
            getattr(event.gift, "diamond_count", None)
            or getattr(event.gift, "diamond_value", None)
            or 0
        )

        total_diamonds = diamond_value * event.repeat_count

        log_gift_to_neon(
            creator_username,
            sender_username,
            sender_display_name,
            gift_name,
            diamond_value,
            event.repeat_count,
            total_diamonds,
        )

        if total_diamonds >= DIAMOND_ALERT_THRESHOLD:
            gift_image = build_gift_image_url(gift_name)
            send_discord_alert(
                creator_username,
                sender_username,
                sender_display_name,
                gift_name,
                diamond_value,
                event.repeat_count,
                total_diamonds,
                gift_image_url=gift_image,
            )

    async def runner():
        try:
            await client.connect()
            await asyncio.wait_for(client.run(), timeout=MAX_RUNTIME_SECONDS)
        except:
            pass
        finally:
            ACTIVE_CLIENTS.pop(creator_username, None)
            print(f"Stopped listener for {creator_username}")
            print(f"Active listeners {len(ACTIVE_CLIENTS)}")

    asyncio.create_task(runner())


async def scan_creators_loop(creators):
    while True:
        for username in creators:
            if username in ACTIVE_CLIENTS:
                await asyncio.sleep(SCAN_DELAY_BETWEEN_CREATORS)
                continue

            try:
                probe = TikTokLiveClient(unique_id=username)
                is_live = await asyncio.wait_for(probe.is_live(), timeout=5)
                try:
                    await probe.disconnect()
                except:
                    pass

                if is_live:
                    touch_activity()
                    await start_listener_for_creator(username)

            except:
                pass

            await asyncio.sleep(SCAN_DELAY_BETWEEN_CREATORS)

        await asyncio.sleep(SCAN_SLEEP_BETWEEN_FULL_PASSES)


async def watchdog_loop():
    while True:
        await asyncio.sleep(30)

        runtime = time.time() - SERVICE_START_TIME
        inactivity = time.time() - LAST_ACTIVITY_TIME

        if runtime > MAX_RUNTIME_SECONDS:
            print("Max runtime reached, exiting for redeploy")
            os._exit(1)

        if inactivity > STALL_TIMEOUT_SECONDS:
            print("No activity detected, exiting for restart")
            os._exit(1)


async def main():
    creators = load_creators()
    await asyncio.gather(
        scan_creators_loop(creators),
        watchdog_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
