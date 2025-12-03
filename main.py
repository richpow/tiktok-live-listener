import os
import asyncio
import psycopg2
import requests
from TikTokLive import TikTokLiveClient
from TikTokLive.events import GiftEvent

DATABASE_URL = os.getenv("DATABASE_URL")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

DIAMOND_ALERT_THRESHOLD = 3000


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
    gift_name,
    diamonds_per_item,
    repeat_count,
    total_diamonds
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
            {"name": "From", "value": sender_username, "inline": False},
            {"name": "Display Name", "value": sender_display_name, "inline": False},
            {"name": "Gift", "value": gift_name, "inline": False},
            {"name": "Diamonds per item", "value": str(diamonds_per_item), "inline": True},
            {"name": "Count", "value": str(repeat_count), "inline": True},
            {"name": "Total diamonds", "value": str(total_diamonds), "inline": False}
        ]
    }

    payload = {"embeds": [embed]}

    try:
        requests.post(DISCORD_WEBHOOK_URL, json=payload)
        print("Discord alert sent")
    except Exception as e:
        print("Discord alert error:", e)


async def run_listener_for_creator(creator_username):
    while True:
        try:
            print(f"Starting listener for {creator_username}")
            client = TikTokLiveClient(unique_id=creator_username)

            @client.on(GiftEvent)
            async def on_gift(event: GiftEvent):
                sender_username = event.user.unique_id
                sender_display = event.user.nickname
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
                print("Display name:", sender_display)
                print("Gift:", gift_name)
                print("Diamonds per item:", diamond_value)
                print("Count:", repeat_count)
                print("Total diamonds:", total_diamonds)
                print("----------------------\n")

                log_gift_to_neon(
                    creator_username,
                    sender_username,
                    sender_display,
                    gift_name,
                    diamond_value,
                    repeat_count,
                    total_diamonds
                )

                if total_diamonds >= DIAMOND_ALERT_THRESHOLD:
                    send_discord_alert(
                        creator_username,
                        sender_username,
                        sender_display,
                        gift_name,
                        diamond_value,
                        repeat_count,
                        total_diamonds
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
                    f"Creator {creator_username} probably not live or TikTok returned bad data. "
                    f"Retrying this creator in 1800 seconds"
                )
                await asyncio.sleep(1800)
            else:
                print(
                    f"Error in listener for {creator_username}: {e}. "
                    f"Restarting this creator in 10 seconds"
                )
                await asyncio.sleep(10)


async def main():
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
