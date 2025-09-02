import asyncio
import datetime
import random
from .client import fetch_repos
from .store import insert_repositories

FIXED_CONCURRENCY = 20
BATCH_SIZE = 2000
GLOBAL_SLEEP_SECS = 4  # pause when quota low / hard limit

buffer = []
buffer_lock = asyncio.Lock()

rate_limit_event = asyncio.Event()
rate_limit_event.set()

stop_event = asyncio.Event()  # signal when target reached


class ConcurrencyController:
    """Fixed concurrency with global sleep on low quota."""
    def __init__(self):
        self.current = FIXED_CONCURRENCY

    async def pre_request(self):
        await rate_limit_event.wait()
        if stop_event.is_set():
            raise asyncio.CancelledError()
        await asyncio.sleep(random.uniform(0.02, 0.05))  # tiny jitter

    async def check_rate_limit(self, remaining: int, reset: int = None):
        if remaining is not None and remaining < 50:
            if rate_limit_event.is_set():
                print(f"⚠️ Low quota (remaining={remaining}), pausing ALL tasks for {GLOBAL_SLEEP_SECS}s...")
                rate_limit_event.clear()
                asyncio.create_task(self._resume_after(GLOBAL_SLEEP_SECS))

    async def rate_limited(self, reset: int = None):
        if rate_limit_event.is_set():
            sleep_for = GLOBAL_SLEEP_SECS if not reset else max(
                1, reset - int(datetime.datetime.now().timestamp()) + 1
            )
            print(f"⏸️ Hard limit hit. Pausing ALL tasks {sleep_for}s...")
            rate_limit_event.clear()
            asyncio.create_task(self._resume_after(sleep_for))

    async def _resume_after(self, sleep_for: int):
        await asyncio.sleep(sleep_for)
        rate_limit_event.set()
        print("✅ Resumed after pause")


class AdaptiveLimiter:
    """Simple fixed concurrency limiter."""
    def __init__(self, controller: ConcurrencyController):
        self.controller = controller
        self.active = 0
        self.lock = asyncio.Lock()

    async def acquire(self):
        while True:
            if stop_event.is_set():
                raise asyncio.CancelledError()
            async with self.lock:
                if self.active < self.controller.current:
                    self.active += 1
                    return
            await asyncio.sleep(0.02)

    async def release(self):
        async with self.lock:
            self.active -= 1


# -------------------------
# Shard building
# -------------------------

def build_star_ranges():
    return ["0..0", "1..10"]


def build_date_ranges(months_back=12, field="created", granularity="month"):
    ranges = []
    today = datetime.date.today().replace(day=1)
    start_date = today - datetime.timedelta(days=30 * months_back)

    cur = start_date
    while cur < today:
        if granularity == "day":
            step = datetime.timedelta(days=1)
            end = cur
        elif granularity == "week":
            step = datetime.timedelta(days=7)
            end = cur + datetime.timedelta(days=6)
        elif granularity == "month":
            step = datetime.timedelta(days=32)
            end = (cur + step).replace(day=1) - datetime.timedelta(days=1)
        else:
            raise ValueError(f"Unknown granularity {granularity}")

        if end > today:
            end = today

        ranges.append(f"{field}:{cur}..{end}")
        cur = end + datetime.timedelta(days=1)

    return ranges


def build_shards(months_back=12):
    shards = []
    for s in build_star_ranges():
        if s == "0..0":
            created_ranges = build_date_ranges(months_back, granularity="day")
        elif s == "1..10":
            created_ranges = build_date_ranges(months_back, granularity="week")
        else:
            created_ranges = build_date_ranges(months_back, granularity="month")

        for c in created_ranges:
            shards.append(f"stars:{s} {c}")

    random.shuffle(shards)
    return shards


# -------------------------
# Crawl
# -------------------------

async def flush_buffer(total_inserted):
    global buffer
    async with buffer_lock:
        if len(buffer) >= BATCH_SIZE:
            inserted = insert_repositories(buffer)
            buffer = []
            total_inserted += inserted
            print(f"📦 Inserted batch of {inserted} repos (total={total_inserted})")
    return total_inserted


async def crawl_shard(shard, controller, limiter, batch_id):
    await limiter.acquire()
    try:
        await controller.pre_request()
        repos, remaining, reset = await fetch_repos(shard)

        if repos:
            async with buffer_lock:
                before = len(buffer)
                buffer.extend(repos)
                after = len(buffer)
            print(
                f"🧩 Shard {batch_id} [{shard}] → "
                f"fetched={len(repos)}, buffer+={after - before}, "
                f"concurrency={controller.current}, remaining={remaining}"
            )
        else:
            print(f"🧩 Shard {batch_id} [{shard}] → fetched=0")

        if len(repos) >= 1000:
            print(f"⚠️ Shard {batch_id} [{shard}] hit 1000-cap (truncated)")

        await controller.check_rate_limit(remaining, reset)
        return repos, remaining, reset

    except Exception as e:
        msg = str(e)
        if "RATE_LIMITED" in msg or "403" in msg:
            # delegate to global pause (no random backoff)
            await controller.rate_limited()
        else:
            backoff = random.uniform(1, 5)
            print(f"⚠️ Error on shard {batch_id} [{shard}]: {e}. Retrying in {backoff:.1f}s...")
            await asyncio.sleep(backoff)
        return [], None, None

    finally:
        await limiter.release()


async def crawl(target_repos=100000, months_back=12):
    global buffer
    start_time = datetime.datetime.now()

    shards = build_shards(months_back)
    print(f"🔪 Built {len(shards)} shuffled shards (stars 0..0 daily, 1..10 weekly)")

    controller = ConcurrencyController()
    limiter = AdaptiveLimiter(controller)

    total_inserted = 0
    batch_id = 0

    tasks = []
    for shard in shards:
        if stop_event.is_set():
            break
        batch_id += 1
        task = asyncio.create_task(crawl_shard(shard, controller, limiter, batch_id))
        tasks.append(task)

    try:
        for coro in asyncio.as_completed(tasks):
            if stop_event.is_set():
                break

            repos, remaining, reset = await coro
            if repos:
                async with buffer_lock:
                    before = len(buffer)
                    buffer.extend(repos)
                total_inserted = await flush_buffer(total_inserted)

            if total_inserted >= target_repos:
                stop_event.set()
                print(f"🎯 Target reached: {total_inserted} repos")
                break
    finally:
        # 🚨 Cancel pending tasks if we reached target early
        if stop_event.is_set():
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    # flush leftovers
    async with buffer_lock:
        if buffer:
            inserted = insert_repositories(buffer)
            total_inserted += inserted
            print(f"📦 Inserted final batch of {inserted} repos (total={total_inserted})")
            buffer = []

    elapsed = (datetime.datetime.now() - start_time).total_seconds()
    print(f"\n🎉 Done! Inserted {total_inserted} unique repos in {elapsed:.2f}s ({elapsed/60:.2f} min)")

