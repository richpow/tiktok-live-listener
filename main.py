import os
import asyncio
import time
import logging
import ssl
import re
import unicodedata
from dataclasses import dataclass, field
from typing import Dict, Optional, List

import aiohttp
import asyncpg
from TikTokLive import TikTokLiveClient
from TikTokLive.events import GiftEvent


DATABASE_URL = os.getenv("DATABASE_URL", "")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")

DIAMOND_ALERT_THRESHOLD = int(os.getenv("DIAMOND_ALERT_THRESHOLD", "9999"))

GITHUB_BASE = "https://raw.githubusercontent.com/richpow/tiktok-live-listener/main/gifts"

STREAMTOEARN_REGION = os.getenv("STREAMTOEARN_REGION", "GB")
STREAMTOEARN_URL = f"https://streamtoearn.io/gifts?region={STREAMTOEARN_REGION}"
STREAMTOEARN_REFRESH_SECONDS = int(os.getenv("STREAMTOEARN_REFRESH_SECONDS", "3600"))

SCAN_DELAY_BETWEEN_CREATORS = float(os.getenv("SCAN_DELAY_BETWEEN_CREATORS", "0.4"))
SCAN_SLEEP_BETWEEN_PASSES = float(os.getenv("SCAN_SLEEP_BETWEEN_PASSES", "12"))

MAX_PROBE_CONCURRENCY = int(os.getenv("MAX_PROBE_CONCURRENCY", "20"))
PROBE_TIMEOUT = float(os.getenv("PROBE_TIMEOUT", "6"))

IDLE_RECONNECT_SECONDS = int(os.getenv("IDLE_RECONNECT_SECONDS", "900"))
CREATOR_REFRESH_SECONDS = int(os.getenv("CREATOR_REFRESH_SECONDS", "600"))


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

logging.getLogger("TikTokLive").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("aiohttp").setLevel(logging.WARNING)

log = logging.getLogger("listener")


@dataclass
class CreatorState:
    username: str
    task: Optional[asyncio.Task] = None
    last_event: float = field(default_factory=time.time)
    stopping: bool = False


def _strip_to_ascii(s: str) -> str:
    if not s:
        return ""
    normalized = unicodedata.normalize("NFKD", s)
    return normalized.encode("ascii", "ignore").decode("ascii")


def _slugify(s: str) -> str:
    s = (s or "").strip().lower()
    s = _strip_to_ascii(s)
    s = s.replace("&", "and")
    s = re.sub(r"[’`´]", "'", s)
    s = s.replace("'", "")
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


class GiftListenerService:
    def __init__(self):
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL missing")

        self.pool: Optional[asyncpg.Pool] = None
        self.http: Optional[aiohttp.ClientSession] = None

        self.creators: List[str] = []
        self.states: Dict[str, CreatorState] = {}

        self.probe_sem = asyncio.Semaphore(MAX_PROBE_CONCURRENCY)

        self.image_cache: Dict[str, Optional[str]] = {}
        self.streamtoearn_map: Dict[str, str] = {}
        self.printed_gift_fields_once = False


    async def start(self):
        ssl_ctx = ssl.create_default_context()

        self.pool = await asyncpg.create_pool(
            dsn=DATABASE_URL,
            ssl=ssl_ctx,
            min_size=1,
            max_size=8,
        )

        self.http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12))

        await self.refresh_creators()
        await self.refresh_streamtoearn_map()

        await asyncio.gather(
            self.creator_refresh_loop(),
            self.streamtoearn_refresh_loop(),
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
        log.info("Loaded %s creators", len(self.creators))


    async def creator_refresh_loop(self):
        while True:
            await asyncio.sleep(CREATOR_REFRESH_SECONDS)
            try:
                await self.refresh_creators()
            except Exception as e:
                log.warning("Creator refresh failed: %s", e)


    async def streamtoearn_refresh_loop(self):
        while True:
            await asyncio.sleep(STREAMTOEARN_REFRESH_SECONDS)
            try:
                await self.refresh_streamtoearn_map()
            except Exception as e:
                log.warning("Streamtoearn refresh failed: %s", e)


    async def refresh_streamtoearn_map(self):
        if not self.http:
            return

        try:
            async with self.http.get(STREAMTOEARN_URL) as resp:
                if resp.status != 200:
                    log.warning("Streamtoearn HTTP %s", resp.status)
                    return
                html = await resp.text()
        except Exception as e:
            log.warning("Streamtoearn fetch error: %s", e)
            return

        mapping: Dict[str, str] = {}

        # Very tolerant parsing:
        # Find pairs of (image url, nearby gift name) in the HTML.
        # This avoids relying on a specific DOM structure that might change.
        #
        # We accept common image extensions and both absolute and relative urls.
        img_re = re.compile(r'src="([^"]+\.(?:png|jpg|jpeg|webp)[^"]*)"', re.IGNORECASE)
        name_re = re.compile(r'>([^<>]{1,80})<')

        imgs = img_re.findall(html)
        if not imgs:
            log.warning("Streamtoearn parse found no images")
            return

        # Create a rolling window over the HTML to associate an image with a nearby label.
        # This is imperfect, but it is better than name to filename guessing alone.
        # We also store by slug so minor punctuation differences still match.
        for m in img_re.finditer(html):
            img_url = m.group(1)

            # Normalize url
            if img_url.startswith("//"):
                img_url = "https:" + img_url
            elif img_url.startswith("/"):
                img_url = "https://streamtoearn.io" + img_url

            # Look ahead for a plausible label
            window = html[m.end(): m.end() + 800]
            nm = name_re.search(window)
            if not nm:
                continue

            label = nm.group(1).strip()
            if not label:
                continue

            slug = _slugify(label)
            if slug and slug not in mapping:
                mapping[slug] = img_url

        if not mapping:
            log.warning("Streamtoearn parse found no name mappings")
            return

        self.streamtoearn_map = mapping
        log.info("Streamtoearn map loaded (%s entries) for region %s", len(mapping), STREAMTOEARN_REGION)


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


    def _extract_image_url_from_gift(self, gift_obj) -> Optional[str]:
        if not gift_obj:
            return None

        candidate_attrs = [
            "image",
            "icon",
            "picture",
            "url",
            "image_url",
            "icon_url",
            "picture_url",
            "gift_picture_url",
        ]

        for attr in candidate_attrs:
            try:
                val = getattr(gift_obj, attr, None)
            except Exception:
                val = None

            if isinstance(val, str) and val.startswith("http"):
                return val

            if val:
                for sub in ["url", "uri"]:
                    try:
                        subval = getattr(val, sub, None)
                    except Exception:
                        subval = None
                    if isinstance(subval, str) and subval.startswith("http"):
                        return subval

                if isinstance(val, dict):
                    for k in ["url", "uri"]:
                        subval = val.get(k)
                        if isinstance(subval, str) and subval.startswith("http"):
                            return subval

        return None


    def _streamtoearn_image_url(self, gift_name: str) -> Optional[str]:
        slug = _slugify(gift_name)
        if not slug:
            return None
        return self.streamtoearn_map.get(slug)


    async def _github_image_url_if_exists(self, gift_name: str) -> Optional[str]:
        if not self.http:
            return None

        if gift_name in self.image_cache:
            return self.image_cache[gift_name]

        key = _slugify(gift_name)
        if not key:
            self.image_cache[gift_name] = None
            return None

        url = f"{GITHUB_BASE}/{key}.png?raw=true"

        try:
            async with self.http.get(url) as resp:
                if resp.status == 200:
                    self.image_cache[gift_name] = url
                    return url
        except Exception:
            pass

        self.image_cache[gift_name] = None
        return None


    async def listener_loop(self, state: CreatorState):
        username = state.username
        log.info("Listening: %s", username)

        while not state.stopping:
            client = TikTokLiveClient(unique_id=username)

            @client.on(GiftEvent)
            async def on_gift(event: GiftEvent):
                state.last_event = time.time()

                if not self.printed_gift_fields_once:
                    self.printed_gift_fields_once = True
                    try:
                        gift_obj = getattr(event, "gift", None)
                        fields = [x for x in dir(gift_obj) if not x.startswith("_")] if gift_obj else []
                        log.info("Gift fields seen: %s", ", ".join(fields[:120]))
                    except Exception:
                        log.info("Could not inspect gift fields")

                diamonds = (
                    getattr(event.gift, "diamond_count", 0)
                    or getattr(event.gift, "diamond_value", 0)
                )
                total = diamonds * event.repeat_count

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
                        creator=username,
                        sender=event.user.unique_id,
                        sender_name=event.user.nickname,
                        gift=event.gift.name,
                        diamonds=total,
                        gift_obj=getattr(event, "gift", None),
                    )

            async def idle_watch():
                while True:
                    await asyncio.sleep(30)
                    if time.time() - state.last_event > IDLE_RECONNECT_SECONDS:
                        await client.disconnect()
                        return

            idle_task = asyncio.create_task(idle_watch())

            try:
                await client.connect()
                await client.run()
            except Exception:
                pass
            finally:
                idle_task.cancel()
                try:
                    await client.disconnect()
                except Exception:
                    pass

                if not state.stopping:
                    await asyncio.sleep(5)

        log.info("Stopped: %s", username)


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
        gift_obj,
    ):
        if not DISCORD_WEBHOOK_URL or not self.http:
            return

        tiktok_image_url = self._extract_image_url_from_gift(gift_obj)
        streamtoearn_image_url = self._streamtoearn_image_url(gift)
        github_image_url = await self._github_image_url_if_exists(gift)

        image_url = tiktok_image_url or streamtoearn_image_url or github_image_url

        embed = {
            "title": "Gift Alert",
            "description": f"{creator} received {gift}",
            "fields": [
                {"name": "From", "value": f"{sender} | {sender_name}"},
                {"name": "Diamonds", "value": f"{int(diamonds or 0):,}"},
            ],
        }

        if image_url:
            embed["thumbnail"] = {"url": image_url}

        try:
            await self.http.post(DISCORD_WEBHOOK_URL, json={"embeds": [embed]})
        except Exception:
            pass


    async def status_loop(self):
        while True:
            await asyncio.sleep(120)
            active = [
                u for u, s in self.states.items()
                if s.task and not s.task.done()
            ]
            if active:
                log.info("Active listeners (%s): %s", len(active), ", ".join(active))
            else:
                log.info("No active listeners")


async def main():
    svc = GiftListenerService()
    try:
        await svc.start()
    finally:
        await svc.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
