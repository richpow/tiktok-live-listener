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

DATABASE_URL = os.getenv("DATABASE_URL", "")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")

DIAMOND_ALERT_THRESHOLD = int(os.getenv("DIAMOND_ALERT_THRESHOLD", "9999"))

GITHUB_BASE = os.getenv(
    "GITHUB_BASE",
    "https://raw.githubusercontent.com/richpow/tiktok-live-listener/main/gifts",
)

SCAN_DELAY_BETWEEN_CREATORS_SECONDS = float(os.getenv("SCAN_DELAY_BETWEEN_CREATORS", "0.25"))
SCAN_SLEEP_BETWEEN_FULL_PASSES_SECONDS = float(os.getenv("SCAN_SLEEP_BETWEEN_FULL_PASSES", "10"))

MAX_CONCURRENT_LIVE_PROBES = int(os.getenv("MAX_CONCURRENT_LIVE_PROBES", "25"))
PROBE_TIMEOUT_SECONDS = float(os.getenv("PROBE_TIMEOUT_SECONDS", "6"))

LISTENER_IDLE_RECONNECT_SECONDS = int(os.getenv("LISTENER_IDLE_RECONNECT_SECONDS", "900"))
LISTENER_CONNECT_TIMEOUT_SECONDS = int(os.getenv("LISTENER_CONNECT_TIMEOUT_SECONDS", "20"))

CREATOR_LIST_REFRESH_SECONDS = int(os.getenv("CREATOR_LIST_REFRESH_SECONDS", "600"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("gift_listener")


@dataclass
class CreatorState:
    username: str
    task: Optional[asyncio.Task] = None
    last_event_ts: float = field(default_factory=lambda: time.time())
    last_connect_ts: float = 0.0
    reconnect_attempts: int = 0
    stopping: bool = False


class GiftListenerService:
    def __init__(self) -> None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL is missing")

        self.pool: Optional[asyncpg.Pool] = None
        self.http: Optional[aiohttp.ClientSession] = None

        self.creators: List[str] = []
        self.states: Dict[str, CreatorState] = {}

        self.probe_sem = asyncio.Semaphore(MAX_CONCURRENT_LIVE_PROBES)
        self.creator_refresh_lock = asyncio.Lock()

        self.gift_url_cache: Dict[str, str] = {}

    async def start(self) -> None:
        self.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
        timeout = aiohttp.ClientTimeout(total=10)
        self.http = aiohttp.ClientSession(timeout=timeout)

        await self.refresh_creators()

        await asyncio.gather(
            self.creator_refresh_loop(),
            self.scan_loop(),
            self.watchdog_loop(),
        )

    async def close(self) -> None:
        for st in list(self.states.values()):
            await self.stop_creator_listener(st.username)

        if self.http:
            await self.http.close()

        if self.pool:
            await self.pool.close()

    async def refresh_creators(self) -> None:
        async with self.creator_refresh_lock:
            assert self.pool is not None
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
            new_list = [r["tiktok_username"] for r in rows]
            self.creators = new_list
            log.info("Loaded %s creators", len(self.creators))

    async def creator_refresh_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(CREATOR_LIST_REFRESH_SECONDS)
                await self.refresh_creators()
            except Exception as e:
                log.exception("Creator refresh loop error: %s", e)

    async def scan_loop(self) -> None:
        while True:
            for username in list(self.creators):
                if username not in self.states:
                    self.states[username] = CreatorState(username=username)

                st = self.states[username]
                if st.task and not st.task.done():
                    await asyncio.sleep(SCAN_DELAY_BETWEEN_CREATORS_SECONDS)
                    continue

                try:
                    is_live = await self.is_creator_live(username)
                    if is_live:
                        await self.start_creator_listener(username)
                except Exception as e:
                    log.debug("Scan error for %s: %s", username, e)

                await asyncio.sleep(SCAN_DELAY_BETWEEN_CREATORS_SECONDS)

            await asyncio.sleep(SCAN_SLEEP_BETWEEN_FULL_PASSES_SECONDS)

    async def is_creator_live(self, username: str) -> bool:
        async with self.probe_sem:
            probe = TikTokLiveClient(unique_id=username)
            try:
                return await asyncio.wait_for(probe.is_live(), timeout=PROBE_TIMEOUT_SECONDS)
            except Exception:
                return False
            finally:
                try:
                    await probe.disconnect()
                except Exception:
                    pass

    async def start_creator_listener(self, username: str) -> None:
        st = self.states.get(username)
        if not st:
            st = CreatorState(username=username)
            self.states[username] = st

        if st.task and not st.task.done():
            return

        st.stopping = False
        st.task = asyncio.create_task(self.listener_supervisor(username))
        log.info("Started supervisor for %s", username)

    async def stop_creator_listener(self, username: str) -> None:
        st = self.states.get(username)
        if not st:
            return
        st.stopping = True
        if st.task and not st.task.done():
            st.task.cancel()
            try:
                await st.task
            except Exception:
                pass
        st.task = None

    async def listener_supervisor(self, username: str) -> None:
        st = self.states[username]
        backoff = 2.0

        while not st.stopping:
            try:
                live_now = await self.is_creator_live(username)
                if not live_now:
                    await asyncio.sleep(15)
                    continue

                st.last_connect_ts = time.time()
                await self.run_listener_once(username)

                st.reconnect_attempts += 1
                backoff = min(60.0, backoff * 1.8)
                await asyncio.sleep(backoff)

            except asyncio.CancelledError:
                return
            except Exception as e:
                st.reconnect_attempts += 1
                backoff = min(60.0, backoff * 1.8)
                log.info("Listener supervisor error for %s: %s", username, e)
                await asyncio.sleep(backoff)

    async def run_listener_once(self, username: str) -> None:
        st = self.states[username]
        client = TikTokLiveClient(unique_id=username)

        @client.on(GiftEvent)
        async def on_gift(event: GiftEvent):
            st.last_event_ts = time.time()

            sender_username = getattr(event.user, "unique_id", "") or ""
            sender_display_name = getattr(event.user, "nickname", "") or ""
            gift_name = getattr(event.gift, "name", "") or ""

            diamond_value = (
                getattr(event.gift, "diamond_count", None)
                or getattr(event.gift, "diamond_value", None)
                or 0
            )
            repeat_count = getattr(event, "repeat_count", 0) or 0
            total_diamonds = int(diamond_value) * int(repeat_count)

            await self.log_gift_to_neon(
                creator_username=username,
                sender_username=sender_username,
                sender_display_name=sender_display_name,
                gift_name=gift_name,
                diamonds_per_item=int(diamond_value),
                repeat_count=int(repeat_count),
                total_diamonds=int(total_diamonds),
            )

            if total_diamonds >= DIAMOND_ALERT_THRESHOLD:
                gift_image_url = self.build_gift_image_url(gift_name)
                await self.send_discord_alert(
                    creator_username=username,
                    sender_username=sender_username,
                    sender_display_name=sender_display_name,
                    gift_name=gift_name,
                    total_diamonds=int(total_diamonds),
                    gift_image_url=gift_image_url,
                )

        async def idle_reconnector():
            while True:
                await asyncio.sleep(30)
                idle_for = time.time() - st.last_event_ts
                if idle_for > LISTENER_IDLE_RECONNECT_SECONDS:
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                    return

        idle_task = asyncio.create_task(idle_reconnector())

        try:
            log.info("Connecting listener for %s", username)
            await asyncio.wait_for(client.connect(), timeout=LISTENER_CONNECT_TIMEOUT_SECONDS)
            await client.run()
        finally:
            idle_task.cancel()
            try:
                await idle_task
            except Exception:
                pass
            try:
                await client.disconnect()
            except Exception:
                pass
            log.info("Listener ended for %s", username)

    async def log_gift_to_neon(
        self,
        creator_username: str,
        sender_username: str,
        sender_display_name: str,
        gift_name: str,
        diamonds_per_item: int,
        repeat_count: int,
        total_diamonds: int,
    ) -> None:
        try:
            assert self.pool is not None
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
                values ($1, $2, $3, $4, $5, $6, $7)
                """,
                creator_username,
                sender_username,
                sender_display_name,
                gift_name,
                diamonds_per_item,
                repeat_count,
                total_diamonds,
            )
        except Exception as e:
            log.debug("Neon insert failed: %s", e)

    def build_gift_image_url(self, gift_name: str) -> Optional[str]:
        if not gift_name:
            return None
        key = gift_name.lower().strip().replace(" ", "_").replace("'", "")
        if key in self.gift_url_cache:
            return self.gift_url_cache[key]
        url = f"{GITHUB_BASE}/{key}.png"
        self.gift_url_cache[key] = url
        return url

    async def send_discord_alert(
        self,
        creator_username: str,
        sender_username: str,
        sender_display_name: str,
        gift_name: str,
        total_diamonds: int,
        gift_image_url: Optional[str],
    ) -> None:
        if not DISCORD_WEBHOOK_URL:
            return
        assert self.http is not None

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
            await self.http.post(DISCORD_WEBHOOK_URL, json={"embeds": [embed]})
        except Exception as e:
            log.debug("Discord webhook failed: %s", e)

    async def watchdog_loop(self) -> None:
        while True:
            await asyncio.sleep(60)

            now = time.time()

            running = 0
            stalled = 0

            for st in self.states.values():
                if st.task and not st.task.done():
                    running += 1
                    idle_for = now - st.last_event_ts
                    if idle_for > LISTENER_IDLE_RECONNECT_SECONDS * 2:
                        stalled += 1

            if stalled > 0:
                log.info("Watchdog, running listeners %s, stalled listeners %s", running, stalled)
            else:
                log.info("Watchdog, running listeners %s", running)


async def main() -> None:
    svc = GiftListenerService()
    try:
        await svc.start()
    finally:
        await svc.close()


if __name__ == "__main__":
    asyncio.run(main())
