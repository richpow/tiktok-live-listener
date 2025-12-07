import os
import asyncio
import psycopg2
import requests
from TikTokLive import TikTokLiveClient
from TikTokLive.events import GiftEvent, ConnectEvent, DisconnectEvent

DATABASE_URL = os.getenv("DATABASE_URL")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

DIAMOND_ALERT_THRESHOLD = 4999
GITHUB_BASE = "https://raw.githubusercontent.com/richpow/tiktok-live-listener/main/gifts"

ACTIVE_LISTENERS = {}
SCAN_INDEX = 0
WINDOW_SIZE = 100
SCAN_INTERVAL = 5


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
    return [r[0] for r in rows]


def log_gift(
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
        print("DB error", e)


def build_gift_image_url(name):
    key = name.lower().strip().replace(" ", "_").replace("'", "")
    url = f"{GITHUB_BASE}/{key}.png"
    r = requests.get(url)
    return url if r.status_code == 200 else None


def send_discord_alert(
    creator_username,
    sender_username,
    sender_display_name,
    gift_name,
    diamonds_per_item,
    repeat_count,
    total
):
    if not DISCORD_WEBHOOK_URL:
        return

    image = build_gift_image_url(gift_name)

    embed = {
        "title": "Gift Alert",
        "description": f"{creator_username} received {gift_name} from {sender_username}",
        "color": 3447003,
        "fields": [
            {"name": "Creator", "value": creator_username, "inline": True},
            {
                "name": "Sender",
                "value": f"{sender_username} | {sender_display_name}",
                "inline": True,
            },
            {"name": "Diamonds", "value": f"{total:,}", "inline": False},
        ],
    }

    if image:
        embed["thumbnail"] = {"url": image}

    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"embeds": [embed]})
    except Exception as e:
        print("Discord error", e)


async def start_listener(username):
    if username in ACTIVE_LISTENERS:
        return

    client = TikTokLiveClient(unique_id=username)
    ACTIVE_LISTENERS[username] = client

    @client.on(GiftEvent)
    async def on_gift(event):
        sender_username = event.user.unique_id
        sender_display_name = event.user.nickname
        gift_name = event.gift.name
        diamonds = getattr(event.gift, "diamond_count", 0)
        count = event.repeat_count
        total = diamonds * count

        log_gift(
            username,
            sender_username,
            sender_display_name,
            gift_name,
            diamonds,
            count,
            total,
        )

        if total >= DIAMOND_ALERT_THRESHOLD:
            send_discord_alert(
                username,
                sender_username,
                sender_display_name,
                gift_name,
                diamonds,
                count,
                total,
            )

    @client.on(DisconnectEvent)
    async def on_disconnect(_):
        if username in ACTIVE_LISTENERS:
            del ACTIVE_LISTENERS[username]

    try:
        await client.connect()
        asyncio.create_task(client.listen())
    except Exception as e:
        if username in ACTIVE_LISTENERS:
            del ACTIVE_LISTENERS[username]


async def scan_creators(creators):
    global SCAN_INDEX

    total = len(creators)

    while True:
        start = SCAN_INDEX
        end = min(SCAN_INDEX + WINDOW_SIZE, total)
        batch = creators[start:end]

        for username in batch:
            if username in ACTIVE_LISTENERS:
                continue

            try:
                probe = TikTokLiveClient(unique_id=username)
                info = await probe.get_room_info()
                if info.get("status") == 1:
                    print(f"{username} is live, starting listener")
                    await start_listener(username)
            except Exception:
                pass

        SCAN_INDEX = end if end < total else 0
        await asyncio.sleep(SCAN_INTERVAL)


async def main():
    creators = load_creators()
    print(f"Loaded {len(creators)} creators")
    asyncio.create_task(scan_creators(creators))
    while True:
        await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
