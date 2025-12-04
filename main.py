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
    check = requests.get(url)
    if check.status_code == 200:
        return url
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
        print("No webhook configured")
        return

    # Compact title line
    description_text = (
        f"**{creator_username}** has just received a **{gift_name}** from **{sender_username}**"
    )

    embed = {
        "title": "Gift Alert",
        "description": description_text,
        "color": 3447003,  # blue accent bar
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

    # Right aligned small icon
    if gift_image_url:
        embed["thumbnail"] = {"url": gift_image_url}

    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"embeds": [embed]})
        print("Discord alert sent")

    except Exception as e:
        print("Discord error:", e)


async def run_listener_for_creator(creator_username):
    while True:
        try:
            print(f"Starting listener for {creator_username}")
            client = TikTokLiveClient(unique_id=creator_username)

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

                # Always log
                log_gift_to_neon(
                    creator_username,
                    sender_username,
                    sender_display_name,
                    gift_name,
                    diamond_value,
                    repeat_count,
                    total_diamonds,
                )

                # Alerts only for threshold
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

            await client.connect()
            await client.listen()

        except Exception as e:
            msg = str(e)

            offline = (
                "UserNotFoundError" in msg
                or "Expecting value" in msg
                or "SIGN_NOT_200" in msg
            )

            if offline:
                print(f"{creator_username} offline, retry in 1800 seconds")
                await asyncio.sleep(1800)
            else:
                print(f"Error for {creator_username}: {e}, retry in 10 seconds")
                await asyncio.sleep(10)


async def main():
    creators = load_creators()

    tasks = [
        asyncio.create_task(run_listener_for_creator(username))
        for username in creators
    ]

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
