import asyncio
import datetime
import random
from .client import fetch_repos
from .store import insert_repositories

MIN_CONCURRENCY = 5
MAX_CONCURRENCY = 30      # keep safer upper bound
START_CONCURRENCY = 10
BATCH_SIZE = 2000

buffer = []
buffer_lock = asyncio.Lock()


class ConcurrencyController:
    def __init__(self, start=START_CONCURRENCY):
        self.current = start

    async def pre_request(self):
        # Jitter sleep before *every* API call to spread load
        await asyncio.sleep(random.uniform(0.2, 0.3))

    async def success(self, batch_id: int):
        # Increase concurrency *slowly* after stable progress
        if batch_id % 50 == 0:
            self.current = min(self.current + 1, MAX_CONCURRENCY)

    async def rate_limited(self):
        # Halve concurrency aggressively on rate limit
        self.current = max(self.current // 2, MIN_CONCURRENCY)
        backoff = random.uniform(5, 30)
        print(f"⚠️ Rate limit hit. Backing off {backoff:.1f}s, concurrency={self.current}")
        await asyncio.sleep(backoff)


def build_star_ranges():
    # Granular at low star counts (where most repos live)
    return [
        "0..1", "2..5", "6..10", "11..20", "21..50",
        "51..100", "101..500", "501..1000",
        "1001..5000", ">5000"
    ]


def build_fork_ranges():
    return [
        "0..1", "2..10", "11..50",
        "51..200", "201..500",
        "501..1000", ">1000"
    ]


def build_date_ranges(months_back=6, field="created"):
    ranges = []
    today = datetime.date.today().replace(day=1)
    for i in range(months_back):
        start = (today - datetime.timedelta(days=30 * i)).replace(day=1)
        end = (start + datetime.timedelta(days=32)).replace(day=1) - datetime.timedelta(days=1)
        ranges.append(f"{field}:{start}..{end}")
    return ranges


def build_shards(months_back=6):
    shards = []
    star_ranges = build_star_ranges()
    fork_ranges = build_fork_ranges()
    created_ranges = build_date_ranges(months_back, field="created")

    for s in star_ranges:
        for f in fork_ranges:
            for c in created_ranges:
                shards.append(f"stars:{s} forks:{f} {c}")

    random.shuffle(shards)  # spread hot/cold load
    return shards


async def flush_buffer():
    global buffer
    async with buffer_lock:
        if len(buffer) >= BATCH_SIZE:
            inserted = insert_repositories(buffer)
            print(f"📦 Inserted batch of {inserted} repos")
            buffer = []


async def crawl_shard(shard, controller, semaphore, batch_id, max_retries=3):
    async with semaphore:
        for attempt in range(1, max_retries + 1):
            try:
                await controller.pre_request()
                repos = await fetch_repos(shard)

                if repos:
                    async with buffer_lock:
                        buffer.extend(repos)
                    await flush_buffer()
                    await controller.success(batch_id)
                    return len(repos)
                return 0

            except Exception as e:
                msg = str(e)
                if "RATE_LIMITED" in msg or "403" in msg:
                    await controller.rate_limited()
                else:
                    backoff = min(30, (2 ** attempt) + random.uniform(0, 3))
                    print(f"⚠️ Error on shard {shard}, attempt {attempt}/{max_retries}: {e}. Retrying in {backoff:.1f}s...")
                    await asyncio.sleep(backoff)

        print(f"❌ Shard {shard} failed after {max_retries} retries")
        return 0


async def crawl(target_repos=100000, months_back=6):
    global buffer
    start_time = datetime.datetime.now()

    shards = build_shards(months_back)
    print(f"🔪 Built {len(shards)} shuffled shards (granular ranges)")

    controller = ConcurrencyController(start=START_CONCURRENCY)
    total_inserted = 0
    batch_id = 0
    semaphore = asyncio.Semaphore(controller.current)

    tasks = []
    for shard in shards:
        batch_id += 1
        tasks.append(crawl_shard(shard, controller, semaphore, batch_id))

    for coro in asyncio.as_completed(tasks):
        inserted = await coro
        total_inserted += inserted
        if total_inserted >= target_repos:
            break

    async with buffer_lock:
        if buffer:
            inserted = insert_repositories(buffer)
            total_inserted += inserted
            print(f"📦 Inserted final batch of {inserted} repos")
            buffer = []

    elapsed = (datetime.datetime.now() - start_time).total_seconds()
    print(f"\n🎉 Done! Inserted {total_inserted} unique repos in {elapsed:.2f}s ({elapsed/60:.2f} min)")

