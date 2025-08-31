import asyncio
import os
from .ingest import crawl

if __name__ == "__main__":
    target = int(os.getenv("TARGET_REPOS", "100000"))
    months_back = int(os.getenv("MONTHS_BACK", "12"))

    print(f"🚀 Starting crawl with TARGET={target}, MONTHS_BACK={months_back}")
    asyncio.run(crawl(target_repos=target, months_back=months_back))
