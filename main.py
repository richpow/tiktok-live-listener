import os
import asyncio
import psycopg2
from TikTokLive import TikTokLiveClient
from TikTokLive.events import GiftEvent

DATABASE_URL = os.getenv("DATABASE_URL")


def get_db():
    return psycopg2.connect(DATABASE_URL)


def load_creators():
    """
    Load all creators from users table where
    tiktok_username is not null or empty
    and creator_id is not null or empty.
    """
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
    total_diamonds,
):
    """
    Insert a gift row into fasttrack_live_gifts.
    """
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
        print(f"Gift logged in Neon for {creator_username}")

    except Exception as e:
        print("Database insert error:", e)


async def run_listener_for_creator(creator_username):
    """
    Runs one listener loop for a single creator.
    If the errors look like offline or bad data,
    wait 30 minutes before trying that creator again.
    Otherwise, short retry.
    """
    while True:
        try:
            print(f"Starting listener for {creator_username}")

            client = TikTokLiveClient(unique_id=creator_username)

            @client.on(GiftEvent)
            async def on_gift(event: GiftEvent):
                sender_username = event.user.unique_id
                sender_display = event.user.nickname
                gift_name = event.gift.name

                # Safe diamond extraction across variants
                diamond_value = None
                if hasattr(event.gift, "diamond_count"):
                    diamond_value = event.gift.diamond_count
                elif hasattr(event.gift, "diamond_value"):
                    diamond_value = event.gift.diamond_value
                elif hasattr(event.gift, "info") and hasattr(
                    event.gift.info, "diamond_count"
                ):
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

                # Log to Neon
                log_gift_to_neon(
                    creator_username=creator_username,
                    sender_username=sender_username,
                    sender_display_name=sender_display,
                    gift_name=gift_name,
                    diamonds_per_item=diamond_value,
                    repeat_count=repeat_count,
                    total_diamonds=total_diamonds,
                )

            await client.connect()
            print(f"Connected. Listening for gifts for {creator_username}")
            await client.listen()

        except Exception as e:
            msg = str(e)
            # Likely offline or bad response cases
            offline_like = (
                "UserNotFoundError" in msg
                or "Expecting value: line 1 column 1" in msg
                or "SIGN_NOT_200" in msg
            )

            if offline_like:
                print(
                    f"Creator {creator_username} probably not live "
                    f"or TikTok returned bad data. "
                    f"Will retry this creator in 1800 seconds"
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
