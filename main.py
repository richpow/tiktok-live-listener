import os
import asyncio
import time
import logging
from dataclasses import dataclass, field
from typing import Dict, Optional, List

import aiohttp
import asyncpg
from TikTokLive import TikTokLiveClient
from TikTokLive.events import GiftEvent


# =========================
# Environment
# =========================

DATABASE_URL = os.getenv("DATABASE_URL", "")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
DIAMOND_ALERT_THRESHOLD = int(os.getenv("DIAMOND_ALERT_THRESHOLD", "9999"))

SCAN_DELAY_BETWEEN_CREATORS = float(os.getenv("SCAN_DELAY_BETWEEN_CREATORS", "0.4"))
SCAN_SLEEP_BETWEEN_PASSES = float(os.getenv("SCAN_SLEEP_BETWEEN_PASSES", "12"))

MAX_PROBE_CONCURRENCY = int(os.getenv("MAX_PROBE_CONCURRENCY", "20"))
PROBE_TIMEOUT = float(os.getenv("PROBE_TIMEOUT", "6"))

IDLE_RECONNECT_SECONDS = int(os.getenv("IDLE_RECONNECT_SECONDS", "900"))
CREATOR_REFRESH_SECONDS = int(os.getenv("CREATOR_REFRESH_SECONDS", "600"))


# =========================
# Logging (STRICT)
# =========================

logging.basicConfig(
    level=logging.WARNING,  # <-- important
    format="%(asctime)s %(levelname)s %(message)s",
)

# Kill noisy libraries completely
logging.getLogger("TikTokLive").setLevel(logging.ERROR)
logging.getLogger("aiohttp").setLevel(logging.ERROR)
logging.getLogger("httpx").setLevel(logging.ERROR)
logging.getLogger("asyncio").setLevel(logging.ERROR)

log = logging.getLogger("listener")


# =========================
# State
# =========================

@dataclass
class CreatorState:
    username: str
    task: Optional[asyncio.Task] = None
    last_event: float = field(default_factory=time.time)
    stopping: bool = False


# =========================
# Service
# =========================

class GiftListenerService:

    def __init__(self):
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL missing")

        self.pool: Optional[asyncpg.Pool] = None
        self.http: Optional[aiohttp.ClientSession] = None

        self.creators: List[str] = []
        self.states: Dict[str, CreatorState] = {}

        self.probe_sem = asyncio.Semaphore(MAX_PROBE_CONCURRENCY)


    async def start(self):
        self.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=8)
        self.http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8))

        await self.refresh_creators()

        await asyncio.gather(
            self.creator_refresh_loop(),
            self.scan_loop(),
            self.status_loop(),
        )


    async def shutdown(self):
        for st in self.states.values():
            st.stopping = True
            if st.task:
                st.task.cancel()

        if self.http:
            await self.http.close()

        if self.pool:
            await self.pool.close()


    async def refresh_creators(self):
        rows = await self.pool.fetch(
            """
            select tiktok_username
            from users
            where tiktok_username is not null
              and tiktok_username <> ''
              and creator_id is not null
              and creator_id <> ''
            """
        )
        self.creators = [r["tiktok_username"] for r in rows]

        # ONE startup log only
        logging.warning("Loaded %s creators", len(self.creators))


    async def creator_refresh_loop(self):
        while True:
            await asyncio.sleep(CREATOR_REFRESH_SECONDS)
            try:
                await self.refresh_creators()
            except Exception:
                pass


    async def scan_loop(self):
        while True:
            for username in self.creators:
                state = self.states.setdefault(username, CreatorState(username))

                if state.task and not state.task.done():
                    continue

                try:
                    if await self.is_live(username):
                        state.task = asyncio.create_task(self.listener_loop(state))
                except Exception:
                    pass

                await asyncio.sleep(SCAN_DELAY_BETWEEN_CREATORS)

            await asyncio.sleep(SCAN_SLEEP_BETWEEN_PASSES)


    async def is_live(self, username: str) -> bool:
        async with self.probe_sem:
            client = TikTokLiveClient(unique_id=username)
            try:
                return await asyncio.wait_for(client.is_live(), timeout=PROBE_TIMEOUT)
            except Exception:
                return False
            finally:
                try:
                    await client.disconnect()
                except Exception:
                    pass


    async def listener_loop(self, state: CreatorState):
        username = state.username

        while not state.stopping:
            client = TikTokLiveClient(unique_id=username)

            @client.on(GiftEvent)
            async def on_gift(event: GiftEvent):
                state.last_event = time.time()

                diamonds = (
                    getattr(event.gift, "diamond_count", 0)
                    or getattr(event.gift, "diamond_value", 0)
                )
                total = diamonds * event.repeat_count

                gift_image_url = None
                if getattr(event.gift, "image", None):
                    urls = getattr(event.gift.image, "url_list", [])
                    if urls:
                        gift_image_url = urls[0]

                await self.log_gift(
                    username,
                    event.user.unique_id,
                    event.user.nickname,
                    event.gift.name,
                    diamonds,
                    event.repeat_count,
                    total,
                )

                if total >= DIAMOND_ALERT_THRESHOLD:
                    await self.send_discord(
                        username,
                        event.user.unique_id,
                        event.user.nickname,
                        event.gift.name,
                        total,
                        gift_image_url,
                    )

            async def idle_watch():
                while True:
                    await asyncio.sleep(30)
                    if time.time() - state.last_event > IDLE_RECONNECT_SECONDS:
                        await client.stop()
                        return

            idle_task = asyncio.create_task(idle_watch())

            try:
                await client.start()  # <-- FIXED coroutine usage
            except Exception:
                pass
            finally:
                idle_task.cancel()
                try:
                    await client.stop()
                except Exception:
                    pass

                if not state.stopping:
                    await asyncio.sleep(5)


    async def log_gift(
        self,
        creator,
        sender,
        sender_name,
        gift,
        per_item,
        count,
        total,
    ):
        try:
            await self.pool.execute(
                """
                insert into fasttrack_live_gifts
                (creator_username,
                 sender_username,
                 sender_display_name,
                 gift_name,
                 diamonds_per_item,
                 repeat_count,
                 total_diamonds)
                values ($1,$2,$3,$4,$5,$6,$7)
                """,
                creator,
                sender,
                sender_name,
                gift,
                per_item,
                count,
                total,
            )
        except Exception:
            pass


    async def send_discord(
        self,
        creator,
        sender,
        sender_name,
        gift,
        diamonds,
        gift_image_url,
    ):
        if not DISCORD_WEBHOOK_URL:
            return

        embed = {
            "title": "Gift Alert",
            "description": f"{creator} received {gift}",
            "fields": [
                {"name": "From", "value": f"{sender} | {sender_name}"},
                {"name": "Diamonds", "value": f"{diamonds:,}"},
            ],
        }

        if gift_image_url:
            embed["thumbnail"] = {"url": gift_image_url}

        try:
            await self.http.post(DISCORD_WEBHOOK_URL, json={"embeds": [embed]})
        except Exception:
            pass


    async def status_loop(self):
        while True:
            await asyncio.sleep(300)
            active = sum(
                1 for s in self.states.values()
                if s.task and not s.task.done()
            )
            logging.warning("Active listeners: %s", active)


# =========================
# Entrypoint
# =========================

async def main():
    svc = GiftListenerService()
    try:
        await svc.start()
    finally:
        await svc.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
